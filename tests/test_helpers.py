"""Unit tests for the pure helpers in main.py.

These avoid AppKit/rumps entirely — they target the functions whose output
drives the menu rendering. If they regress, the menu silently shows wrong
text; the running app itself wouldn't crash, so these tests are the only
safety net.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from flyte.models import ActionPhase

import main


# ---------- _humanize_age ----------

class TestHumanizeAge:
    def test_none_returns_empty(self):
        assert main._humanize_age(None) == ""

    @pytest.mark.parametrize(
        "delta,expected",
        [
            (timedelta(seconds=0), "0s ago"),
            (timedelta(seconds=5), "5s ago"),
            (timedelta(seconds=59), "59s ago"),
            (timedelta(seconds=60), "1m ago"),
            (timedelta(minutes=30), "30m ago"),
            (timedelta(minutes=59, seconds=59), "59m ago"),
            (timedelta(hours=1), "1h ago"),
            (timedelta(hours=23, minutes=59), "23h ago"),
            (timedelta(days=1), "1d ago"),
            (timedelta(days=14), "14d ago"),
        ],
    )
    def test_buckets(self, delta, expected):
        now = datetime.now(timezone.utc)
        assert main._humanize_age(now - delta) == expected


# ---------- _parse_ts ----------

class TestParseTs:
    def test_none(self):
        assert main._parse_ts(None) is None

    def test_empty(self):
        assert main._parse_ts("") is None

    def test_garbage(self):
        assert main._parse_ts("not a timestamp") is None

    def test_z_suffix(self):
        # Flyte timestamps come back ISO-8601 with trailing "Z".
        dt = main._parse_ts("2024-01-02T03:04:05Z")
        assert dt == datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

    def test_explicit_offset(self):
        dt = main._parse_ts("2024-01-02T03:04:05+00:00")
        assert dt == datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


# ---------- _window_hours ----------

class TestWindowHours:
    def test_known_label(self):
        assert main._window_hours("Last 1 hour") == 1
        assert main._window_hours("Last 24 hours") == 24

    def test_ever_returns_none(self):
        assert main._window_hours("Ever") is None

    def test_unknown_label(self):
        # Unknown labels are treated as "Ever" (None means no filter).
        assert main._window_hours("Last 99 fortnights") is None


# ---------- _phase_status_suffix ----------

class TestPhaseStatusSuffix:
    def _t(self, **kwargs) -> datetime:
        return datetime.now(timezone.utc) - timedelta(**kwargs)

    def test_terminal_uses_end_time(self):
        # SUCCEEDED -> "succeeded {end_age}", not the start time.
        suffix = main._phase_status_suffix(
            ActionPhase.SUCCEEDED,
            started=self._t(minutes=10),
            ended=self._t(minutes=1),
        )
        assert suffix == "succeeded 1m ago"

    def test_running_uses_start_time(self):
        suffix = main._phase_status_suffix(
            ActionPhase.RUNNING,
            started=self._t(minutes=5),
            ended=None,
        )
        assert suffix == "started 5m ago"

    def test_failed_verb(self):
        suffix = main._phase_status_suffix(
            ActionPhase.FAILED,
            started=None,
            ended=self._t(hours=2),
        )
        assert suffix == "failed 2h ago"

    def test_no_time_falls_back_to_verb_only(self):
        # If both times are missing, return just the verb — no trailing
        # whitespace from a blank age.
        assert (
            main._phase_status_suffix(ActionPhase.QUEUED, None, None)
            == "queued"
        )


# ---------- _menu_names ----------

class TestMenuNames:
    def test_empty(self):
        assert main._menu_names([], None) == []

    def test_current_only(self):
        # No items from the cluster yet, but we still know our own pick —
        # show it so the radio is correct.
        assert main._menu_names([], "my-proj") == ["my-proj"]

    def test_items_only(self):
        assert main._menu_names(["b", "a", "c"], None) == ["a", "b", "c"]

    def test_dedupes(self):
        assert main._menu_names(["a", "a", "b"], "a") == ["a", "b"]

    def test_includes_current_when_missing_from_items(self):
        # Listing failed or hasn't returned yet — current pick still wins.
        assert main._menu_names(["a", "b"], "c") == ["a", "b", "c"]

    def test_none_current_is_ignored(self):
        # Avoid putting `None` into the menu when nothing has been picked.
        assert main._menu_names(["a"], None) == ["a"]


# ---------- _load_config / _save_config ----------

class TestConfigRoundTrip:
    @pytest.fixture
    def cfg_path(self, tmp_path, monkeypatch):
        path = tmp_path / "config.json"
        monkeypatch.setattr(main, "CONFIG_PATH", path)
        return path

    def test_missing_file_returns_defaults(self, cfg_path):
        project, domain, window = main._load_config()
        assert project is None
        assert domain is None
        assert window == main.DEFAULT_WINDOW_LABEL

    def test_corrupt_file_returns_defaults(self, cfg_path):
        cfg_path.write_text("not json {{{")
        assert main._load_config() == (None, None, main.DEFAULT_WINDOW_LABEL)

    def test_save_and_load(self, cfg_path):
        main._save_config("proj", "dev", "Last 1 hour")
        assert main._load_config() == ("proj", "dev", "Last 1 hour")

    def test_save_without_scope_drops_keys(self, cfg_path):
        # Window-only save shouldn't write a half-populated project/domain.
        main._save_config(None, None, "Last 6 hours")
        data = json.loads(cfg_path.read_text())
        assert data == {"window": "Last 6 hours"}

    def test_unknown_window_falls_back_to_default(self, cfg_path):
        cfg_path.write_text(json.dumps({"window": "Last 99 fortnights"}))
        _, _, window = main._load_config()
        assert window == main.DEFAULT_WINDOW_LABEL

    def test_creates_parent_directory(self, tmp_path, monkeypatch):
        # First-run case: ~/.config/union-status/ doesn't exist yet.
        nested = tmp_path / "fresh" / "config.json"
        monkeypatch.setattr(main, "CONFIG_PATH", nested)
        main._save_config("p", "d", main.DEFAULT_WINDOW_LABEL)
        assert nested.exists()
