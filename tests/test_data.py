"""Stub-based tests for the SDK adapters in main.py.

These cover the seam between the Flyte v2 client (Run/App/Project) and our
internal dataclasses. We don't hit a real cluster — instead we build duck-typed
stubs that match the shapes the SDK actually returns (.to_dict() for
Run/Project, .pb2 attribute trees for App). If Union changes those shapes the
tests still pass; this guards against *us* breaking the conversion.
"""
from __future__ import annotations

import types
from datetime import datetime, timedelta, timezone

from flyte.models import ActionPhase

import main
from main import AppRow, RunRow


# ---------- shared stub builders ----------

def _ns(**kwargs):
    return types.SimpleNamespace(**kwargs)


def _stub_project(pid, domains):
    """Mimic flyte.remote.Project: only the to_dict() shape we read."""
    return _ns(to_dict=lambda: {"id": pid, "domains": [{"id": d} for d in domains]})


def _stub_run(
    name="r1",
    phase=ActionPhase.RUNNING,
    url="https://x/run/r1",
    task="train",
    start="2024-01-01T00:00:00Z",
    end=None,
):
    return _ns(
        name=name,
        phase=phase,
        url=url,
        to_dict=lambda: {
            "action": {
                "metadata": {"task": {"shortName": task}},
                "status": {"startTime": start, "endTime": end},
            }
        },
    )


def _stub_app(
    project="proj",
    domain="dev",
    name="serve",
    endpoint="https://app.example",
    console_url="https://union/app",
    active=True,
    current_replicas=2,
    max_replicas=10,
    conditions=(),
):
    pb2 = _ns(
        metadata=_ns(id=_ns(project=project, domain=domain)),
        status=_ns(
            current_replicas=current_replicas,
            conditions=list(conditions),
        ),
        spec=_ns(autoscaling=_ns(replicas=_ns(max=max_replicas))),
    )
    return _ns(
        is_active=lambda: active,
        name=name,
        endpoint=endpoint,
        url=console_url,
        pb2=pb2,
    )


def _stub_condition(deployment_status, seconds=0, nanos=0):
    return _ns(
        deployment_status=deployment_status,
        last_transition_time=_ns(seconds=seconds, nanos=nanos),
    )


# ---------- _project_pairs ----------

class TestProjectPairs:
    def test_flattens_to_pairs(self):
        projects = [
            _stub_project("alpha", ["development", "production"]),
            _stub_project("beta", ["development"]),
        ]
        assert main._project_pairs(projects) == [
            ("alpha", "development"),
            ("alpha", "production"),
            ("beta", "development"),
        ]

    def test_falls_back_to_development_when_domains_missing(self):
        # API sometimes returns a project with no domains array; the picker
        # still needs *something* clickable.
        proj = _ns(to_dict=lambda: {"id": "lonely"})
        assert main._project_pairs([proj]) == [("lonely", "development")]

    def test_falls_back_to_development_for_empty_domain_list(self):
        proj = _ns(to_dict=lambda: {"id": "lonely", "domains": []})
        assert main._project_pairs([proj]) == [("lonely", "development")]

    def test_drops_entries_with_missing_ids(self):
        # Defensive: a malformed entry shouldn't poison the whole listing.
        projects = [
            _stub_project("ok", ["dev"]),
            _ns(to_dict=lambda: {"domains": [{"id": "dev"}]}),  # no id
            _ns(to_dict=lambda: {"id": "missing-domain", "domains": [{}]}),
        ]
        assert main._project_pairs(projects) == [("ok", "dev")]

    def test_accepts_name_as_id_fallback(self):
        proj = _ns(to_dict=lambda: {"name": "by-name", "domains": [{"name": "dev"}]})
        assert main._project_pairs([proj]) == [("by-name", "dev")]

    def test_empty(self):
        assert main._project_pairs([]) == []


# ---------- _run_row ----------

class TestRunRow:
    def test_basic(self):
        row = main._run_row(
            _stub_run(name="r-42", task="train_model"),
            project="proj",
            domain="dev",
        )
        assert row == RunRow(
            project="proj",
            domain="dev",
            name="r-42",
            phase=ActionPhase.RUNNING,
            started=datetime(2024, 1, 1, tzinfo=timezone.utc),
            ended=None,
            url="https://x/run/r1",
            task="train_model",
        )

    def test_empty_task_name_when_metadata_missing(self):
        run = _ns(
            name="r",
            phase=ActionPhase.SUCCEEDED,
            url="https://x",
            to_dict=lambda: {"action": {"status": {}}},
        )
        row = main._run_row(run, "p", "d")
        assert row.task == ""
        assert row.started is None
        assert row.ended is None

    def test_to_dict_exception_is_swallowed(self):
        # _parse_times and _task_name both catch their own exceptions, so a
        # broken to_dict() shouldn't crash the refresh.
        def _boom():
            raise RuntimeError("nope")

        run = _ns(
            name="r", phase=ActionPhase.FAILED, url="https://x", to_dict=_boom
        )
        row = main._run_row(run, "p", "d")
        assert row.task == ""
        assert row.started is None and row.ended is None


# ---------- _app_row ----------

class TestAppRow:
    def test_basic(self):
        app = _stub_app(
            project="proj",
            domain="dev",
            name="serve",
            endpoint="https://app",
            console_url="https://union/serve",
            current_replicas=3,
            max_replicas=8,
        )
        row = main._app_row(app)
        assert row == AppRow(
            project="proj",
            domain="dev",
            name="serve",
            endpoint="https://app",
            console_url="https://union/serve",
            last_deployed=None,  # no conditions
            current_replicas=3,
            max_replicas=8,
        )

    def test_picks_up_last_deploy_from_conditions(self):
        # A non-steady-state condition (DEPLOYING == status 3) marks the most
        # recent rollout; that's the timestamp the menu should report.
        app = _stub_app(
            conditions=[
                _stub_condition(3, seconds=1_700_000_000),  # DEPLOYING
                _stub_condition(7, seconds=1_700_000_500),  # ACTIVE (steady)
            ]
        )
        row = main._app_row(app)
        assert row.last_deployed == datetime.fromtimestamp(
            1_700_000_000, tz=timezone.utc
        )


# ---------- _group_runs ----------

class TestGroupRuns:
    def _row(self, name, task, started_ago_sec, phase=ActionPhase.SUCCEEDED):
        # Use SUCCEEDED + ended=started so last_activity() doesn't return "now".
        when = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(
            seconds=started_ago_sec
        )
        return RunRow(
            project="p",
            domain="d",
            name=name,
            phase=phase,
            started=when,
            ended=when,
            url=f"https://x/{name}",
            task=task,
        )

    def test_empty(self):
        assert main._group_runs([]) == []

    def test_groups_by_task_and_orders_within(self):
        rows = [
            self._row("old", "train", 600),
            self._row("new", "train", 60),
        ]
        groups = main._group_runs(rows)
        assert len(groups) == 1
        task, rs = groups[0]
        assert task == "train"
        # Newest first within the group.
        assert [r.name for r in rs] == ["new", "old"]

    def test_orders_groups_by_most_recent_activity(self):
        rows = [
            self._row("a1", "alpha", 60),    # most recent alpha
            self._row("a2", "alpha", 9000),
            self._row("b1", "beta", 600),    # most recent beta
            self._row("b2", "beta", 9000),
        ]
        groups = main._group_runs(rows)
        # alpha's newest (60s) beats beta's newest (600s).
        assert [task for task, _ in groups] == ["alpha", "beta"]

    def test_falls_back_to_run_name_when_task_missing(self):
        rows = [self._row("solo", "", 60)]
        task, _ = main._group_runs(rows)[0]
        # No grouping key collapse: the run name becomes the label.
        assert task == "solo"

    def test_running_uses_now_for_activity(self):
        # A RUNNING row with a stale started time still sorts as "most recent"
        # because last_activity() returns now() for in-flight runs.
        ancient_running = self._row(
            "live", "alpha", 100_000, phase=ActionPhase.RUNNING
        )
        fresh_finished = self._row("done", "beta", 10)
        groups = main._group_runs([ancient_running, fresh_finished])
        assert [task for task, _ in groups] == ["alpha", "beta"]


# ---------- _pb_timestamp_to_datetime ----------

class TestPbTimestamp:
    def test_zero_returns_none(self):
        assert main._pb_timestamp_to_datetime(_ns(seconds=0, nanos=0)) is None

    def test_seconds_only(self):
        dt = main._pb_timestamp_to_datetime(_ns(seconds=1_700_000_000, nanos=0))
        assert dt == datetime.fromtimestamp(1_700_000_000, tz=timezone.utc)

    def test_nanos_contribute(self):
        dt = main._pb_timestamp_to_datetime(
            _ns(seconds=1_700_000_000, nanos=500_000_000)
        )
        assert dt.microsecond == 500_000


# ---------- _app_last_deploy_time ----------

class TestAppLastDeployTime:
    def test_empty(self):
        assert main._app_last_deploy_time(_ns(conditions=[])) is None

    def test_returns_most_recent_non_steady_condition(self):
        # Iterates in reverse, so DEPLOYING here (status 3) is the answer
        # even though ACTIVE (7) is more recent — ACTIVE doesn't mark a deploy.
        status = _ns(conditions=[
            _stub_condition(3, seconds=100),  # DEPLOYING
            _stub_condition(7, seconds=200),  # ACTIVE (steady)
        ])
        assert main._app_last_deploy_time(status) == datetime.fromtimestamp(
            100, tz=timezone.utc
        )

    def test_falls_back_to_last_condition_if_all_steady(self):
        # Pure steady-state history: the most recent timestamp is the best
        # signal we've got.
        status = _ns(conditions=[
            _stub_condition(7, seconds=100),
            _stub_condition(8, seconds=200),  # SCALING_UP
        ])
        assert main._app_last_deploy_time(status) == datetime.fromtimestamp(
            200, tz=timezone.utc
        )


