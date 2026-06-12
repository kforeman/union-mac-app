"""Unit tests for the project-picker activity-dot aggregation helper."""

from datetime import datetime, timedelta, timezone

from flyte.models import ActionPhase

from main import ProjectActivity, _project_dot_phase


NOW = datetime(2026, 6, 9, 12, 0, 0, tzinfo=timezone.utc)


def _ago(**kw) -> datetime:
    return NOW - timedelta(**kw)


def test_empty_entries_is_idle():
    assert _project_dot_phase([], window_hours=24, now=NOW) is None


def test_terminal_run_within_window_returns_its_phase():
    entries = [ProjectActivity(phase=ActionPhase.SUCCEEDED, last_activity=_ago(minutes=30))]
    assert _project_dot_phase(entries, window_hours=1, now=NOW) is ActionPhase.SUCCEEDED


def test_terminal_run_outside_window_is_idle():
    entries = [ProjectActivity(phase=ActionPhase.SUCCEEDED, last_activity=_ago(hours=5))]
    assert _project_dot_phase(entries, window_hours=1, now=NOW) is None


def test_newest_terminal_run_wins():
    entries = [
        ProjectActivity(phase=ActionPhase.SUCCEEDED, last_activity=_ago(minutes=10)),
        ProjectActivity(phase=ActionPhase.FAILED, last_activity=_ago(minutes=5)),
    ]
    assert _project_dot_phase(entries, window_hours=1, now=NOW) is ActionPhase.FAILED


def test_running_counts_as_now_even_if_old_or_undated():
    # A run created long ago but still RUNNING is active "now", so it should
    # show in any window regardless of its last_activity timestamp.
    old = [ProjectActivity(phase=ActionPhase.RUNNING, last_activity=_ago(days=30))]
    assert _project_dot_phase(old, window_hours=1, now=NOW) is ActionPhase.RUNNING

    undated = [ProjectActivity(phase=ActionPhase.RUNNING, last_activity=None)]
    assert _project_dot_phase(undated, window_hours=1, now=NOW) is ActionPhase.RUNNING


def test_running_dominates_a_more_recent_terminal_run():
    # Mirrors the menu-bar rule: anything running takes priority over a run
    # that finished more recently.
    entries = [
        ProjectActivity(phase=ActionPhase.RUNNING, last_activity=_ago(days=2)),
        ProjectActivity(phase=ActionPhase.SUCCEEDED, last_activity=_ago(minutes=1)),
    ]
    assert _project_dot_phase(entries, window_hours=1, now=NOW) is ActionPhase.RUNNING


def test_ever_window_returns_phase_for_any_old_activity():
    entries = [ProjectActivity(phase=ActionPhase.ABORTED, last_activity=_ago(days=400))]
    assert _project_dot_phase(entries, window_hours=None, now=NOW) is ActionPhase.ABORTED
