"""Union cluster status — macOS menu bar app.

Polls the Flyte v2 API for recent runs in user-selected project/domain pairs
and shows a traffic-light summary in the macOS menu bar. Click a run to open
it in the Union v2 console.
"""

from __future__ import annotations

import json
import threading
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import re
from concurrent.futures import ThreadPoolExecutor

import flyte
import rumps
from AppKit import (
    NSApplication,
    NSBezierPath,
    NSColor,
    NSCompositingOperationSourceOver,
    NSFont,
    NSForegroundColorAttributeName,
    NSImage,
)
from Foundation import (
    NSAffineTransform,
    NSAttributedString,
    NSMakeRect,
    NSMutableAttributedString,
    NSSize,
)
from flyte.models import ActionPhase
from flyte.remote import Project, Run

REFRESH_SECONDS = 60
RECENT_LIMIT_PER_DOMAIN = 100
TOTAL_SHOWN = 15
CONFIG_PATH = Path.home() / ".config" / "union-status" / "config.json"
UNION_CONFIG_PATH = Path.home() / ".union" / "config.yaml"


def _default_filter_from_union_config() -> Optional[tuple[str, str]]:
    """Pull default (project, domain) from ~/.union/config.yaml (the same file
    used by the Union CLI). Returns None if the file is missing/unparseable."""
    try:
        import yaml

        data = yaml.safe_load(UNION_CONFIG_PATH.read_text()) or {}
    except Exception:
        return None
    task = data.get("task") or {}
    project = task.get("project")
    domain = task.get("domain")
    if project and domain:
        return (project, domain)
    return None

# (label, hours or None for "ever"). First item is the default.
TIME_WINDOWS: list[tuple[str, Optional[float]]] = [
    ("Last 1 hour", 1),
    ("Last 6 hours", 6),
    ("Last 12 hours", 12),
    ("Last 24 hours", 24),
    ("Last 3 days", 72),
    ("Last 1 week", 168),
    ("Ever", None),
]
DEFAULT_WINDOW_LABEL = "Last 24 hours"

TERMINAL_PHASES = {
    ActionPhase.SUCCEEDED,
    ActionPhase.FAILED,
    ActionPhase.ABORTED,
    ActionPhase.TIMED_OUT,
}

PHASE_ICON = {
    ActionPhase.SUCCEEDED: "✅",
    ActionPhase.FAILED: "❌",
    ActionPhase.ABORTED: "⛔️",
    ActionPhase.TIMED_OUT: "⏰",
    ActionPhase.RUNNING: "🔄",
    ActionPhase.INITIALIZING: "🔄",
    ActionPhase.WAITING_FOR_RESOURCES: "⏳",
    ActionPhase.QUEUED: "⏳",
}

# Flat colored dot per phase, rendered via NSAttributedString (no emoji 3D).
# User-assigned colors: green=succeeded, blue=running, orange=aborted,
# red=failed, purple=queued.
DOT_CHAR = "●"
DOT_STRIP_MAX = 10


def _rgb(r: int, g: int, b: int):
    return NSColor.colorWithSRGBRed_green_blue_alpha_(
        r / 255.0, g / 255.0, b / 255.0, 1.0
    )


PHASE_COLOR = {
    ActionPhase.SUCCEEDED: _rgb(52, 199, 89),   # green
    ActionPhase.RUNNING: _rgb(10, 132, 255),    # blue
    ActionPhase.INITIALIZING: _rgb(10, 132, 255),
    ActionPhase.ABORTED: _rgb(255, 149, 0),     # orange
    ActionPhase.FAILED: _rgb(255, 69, 58),      # red
    ActionPhase.TIMED_OUT: _rgb(255, 69, 58),
    ActionPhase.QUEUED: _rgb(191, 90, 242),     # purple
    ActionPhase.WAITING_FOR_RESOURCES: _rgb(191, 90, 242),
}


def _attr(text: str, color=None, font=None) -> NSAttributedString:
    attrs: dict = {}
    if color is not None:
        attrs[NSForegroundColorAttributeName] = color
    if font is not None:
        attrs["NSFont"] = font
    return NSAttributedString.alloc().initWithString_attributes_(text, attrs)


def _build_group_title(task: str, phases: list[ActionPhase]) -> NSAttributedString:
    """Render `{task}   ●●●…` with the dots in their phase colors."""
    out = NSMutableAttributedString.alloc().init()
    out.appendAttributedString_(_attr(f"{task}   "))
    dot_font = NSFont.menuFontOfSize_(0)
    for p in phases:
        color = PHASE_COLOR.get(p)
        out.appendAttributedString_(_attr(DOT_CHAR, color=color, font=dot_font))
        out.appendAttributedString_(_attr(" "))
    return out


def _build_title(icon_phase, label: str) -> NSAttributedString:
    out = NSMutableAttributedString.alloc().init()
    if icon_phase is None:
        out.appendAttributedString_(
            _attr("○ ", color=NSColor.secondaryLabelColor())
        )
    else:
        color = PHASE_COLOR.get(icon_phase, NSColor.labelColor())
        out.appendAttributedString_(_attr(DOT_CHAR + " ", color=color))
    out.appendAttributedString_(_attr(label))
    return out


# ---------- Union logo icon (rendered from embedded SVG paths) ----------

UNION_VIEWBOX = (55.0, 44.0)
UNION_SVG_PATHS = (
    "M22.2368 39.7661C14.124 39.7661 7.5 33.509 7.5 25.2964V10.9941H15.5802"
    "V30.4138C15.5802 31.3075 16.077 31.8102 16.9602 31.8102H27.5132C28.3963"
    " 31.8102 28.8931 31.3075 28.8931 30.4138V10.9941H36.9737V25.2964C36.9737"
    " 33.509 30.3504 39.7661 22.2368 39.7661Z",
    "M32.7631 0C40.876 0 47.5 6.25701 47.5 14.4695V28.7719H39.4198V9.35214"
    "C39.4198 8.45849 38.923 7.95579 38.0398 7.95579H27.4868C26.6037 7.95579"
    " 26.1069 8.45849 26.1069 9.35214V28.7719H18.0263V14.4695C18.0263 6.25701"
    " 24.6496 0 32.7631 0Z",
)

_CMD_RE = re.compile(r"([MLHVCZmlhvcz])([^MLHVCZmlhvcz]*)")
_NUM_RE = re.compile(r"-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?")


def _bezier_from_svg(d: str) -> NSBezierPath:
    path = NSBezierPath.bezierPath()
    x = y = sx = sy = 0.0
    for match in _CMD_RE.finditer(d):
        cmd = match.group(1)
        nums = [float(n) for n in _NUM_RE.findall(match.group(2))]
        if cmd == "M":
            x, y = nums[0], nums[1]
            path.moveToPoint_((x, y))
            sx, sy = x, y
            for i in range(2, len(nums), 2):
                x, y = nums[i], nums[i + 1]
                path.lineToPoint_((x, y))
        elif cmd == "L":
            for i in range(0, len(nums), 2):
                x, y = nums[i], nums[i + 1]
                path.lineToPoint_((x, y))
        elif cmd == "H":
            for nx in nums:
                x = nx
                path.lineToPoint_((x, y))
        elif cmd == "V":
            for ny in nums:
                y = ny
                path.lineToPoint_((x, y))
        elif cmd == "C":
            for i in range(0, len(nums), 6):
                cp1 = (nums[i], nums[i + 1])
                cp2 = (nums[i + 2], nums[i + 3])
                end = (nums[i + 4], nums[i + 5])
                path.curveToPoint_controlPoint1_controlPoint2_(end, cp1, cp2)
                x, y = end
        elif cmd == "Z":
            path.closePath()
            x, y = sx, sy
    return path


def build_union_icon(height_pt: float = 20.0, phase_color=None) -> NSImage:
    """Render the Union logo as an NSImage sized for the menu bar, with an
    optional flat colored dot overlaid in the bottom-right corner."""
    vb_w, vb_h = UNION_VIEWBOX
    # The actual glyph sits inside the viewbox with padding; use the viewbox
    # so the dot has a natural corner to sit in.
    aspect = vb_w / vb_h
    size = NSSize(height_pt * aspect, height_pt)

    image = NSImage.alloc().initWithSize_(size)
    image.lockFocus()
    try:
        # Flip Y and scale viewbox into target size.
        sx = size.width / vb_w
        sy = size.height / vb_h
        scale = min(sx, sy)
        tx = NSAffineTransform.transform()
        tx.translateXBy_yBy_(
            (size.width - vb_w * scale) / 2,
            (size.height - vb_h * scale) / 2 + vb_h * scale,
        )
        tx.scaleXBy_yBy_(scale, -scale)

        # Draw the logo monochrome, tinted to the current menu bar text color.
        # labelColor is dynamic, so .set() resolves to black in light mode
        # and white in dark mode.
        NSColor.labelColor().set()
        for d in UNION_SVG_PATHS:
            p = _bezier_from_svg(d)
            p.transformUsingAffineTransform_(tx)
            p.fill()

        if phase_color is not None:
            # Small colored dot in the bottom-right corner. A 1pt halo in the
            # menu bar's background colour separates it from the logo stroke.
            dot_d = height_pt * 0.5
            halo_d = dot_d + 1.5
            halo_rect = NSMakeRect(
                size.width - halo_d, -0.5, halo_d, halo_d
            )
            dot_rect = NSMakeRect(
                size.width - dot_d - 0.75, 0.25, dot_d, dot_d
            )
            NSColor.windowBackgroundColor().set()
            NSBezierPath.bezierPathWithOvalInRect_(halo_rect).fill()
            phase_color.set()
            NSBezierPath.bezierPathWithOvalInRect_(dot_rect).fill()
    finally:
        image.unlockFocus()

    image.setTemplate_(False)
    return image


@dataclass
class RunRow:
    project: str
    domain: str
    name: str
    phase: ActionPhase
    started: Optional[datetime]
    ended: Optional[datetime]
    url: str
    task: str

    @property
    def icon(self) -> str:
        return PHASE_ICON.get(self.phase, "•")

    @property
    def is_running(self) -> bool:
        return self.phase not in TERMINAL_PHASES

    def last_activity(self) -> Optional[datetime]:
        if self.is_running:
            return datetime.now(timezone.utc)
        return self.ended or self.started


def _parse_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _parse_times(run: Run) -> tuple[Optional[datetime], Optional[datetime]]:
    try:
        status = run.to_dict().get("action", {}).get("status", {}) or {}
        return _parse_ts(status.get("startTime")), _parse_ts(status.get("endTime"))
    except Exception:
        return None, None


def _task_name(run: Run) -> str:
    try:
        meta = run.to_dict().get("action", {}).get("metadata", {})
        return meta.get("task", {}).get("shortName") or meta.get("funtionName") or ""
    except Exception:
        return ""


def _humanize_age(dt: Optional[datetime]) -> str:
    if dt is None:
        return ""
    s = int((datetime.now(timezone.utc) - dt).total_seconds())
    if s < 60:
        return f"{s}s ago"
    if s < 3600:
        return f"{s // 60}m ago"
    if s < 86400:
        return f"{s // 3600}h ago"
    return f"{s // 86400}d ago"


def _load_config() -> tuple[list[tuple[str, str]], str]:
    labels = {label for label, _ in TIME_WINDOWS}
    default_pair = _default_filter_from_union_config()
    fallback_pairs: list[tuple[str, str]] = [default_pair] if default_pair else []
    try:
        data = json.loads(CONFIG_PATH.read_text())
    except FileNotFoundError:
        return fallback_pairs, DEFAULT_WINDOW_LABEL
    except Exception:
        return fallback_pairs, DEFAULT_WINDOW_LABEL
    pairs = [(f["project"], f["domain"]) for f in data.get("filters", [])]
    window = data.get("window")
    if window not in labels:
        window = DEFAULT_WINDOW_LABEL
    return (pairs if pairs else fallback_pairs), window


def _save_config(
    filters: list[tuple[str, str]], window_label: str
) -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(
        json.dumps(
            {
                "filters": [{"project": p, "domain": d} for p, d in filters],
                "window": window_label,
            },
            indent=2,
        )
    )


def _window_hours(label: str) -> Optional[float]:
    for l, h in TIME_WINDOWS:
        if l == label:
            return h
    return None


class UnionStatusApp(rumps.App):
    def __init__(self) -> None:
        super().__init__("Union", title="⚪ Union", quit_button=None)
        self.host: str = ""
        self.runs: list[RunRow] = []
        self.available_pairs: list[tuple[str, str]] = []
        self.last_activity: dict[tuple[str, str], datetime] = {}
        filters, window = _load_config()
        self.filters: set[tuple[str, str]] = set(filters)
        self.window_label: str = window
        self.error: Optional[str] = None
        self.last_refresh: Optional[datetime] = None
        self._lock = threading.Lock()
        self._pending_render = False
        self._flyte_ready = False

        self._projects_menu = rumps.MenuItem("Projects")
        self._window_menu = rumps.MenuItem("Time window")
        self.menu = [
            "Loading…",
            None,
            self._projects_menu,
            self._window_menu,
            rumps.MenuItem("Refresh now", callback=self._on_refresh_click),
            rumps.MenuItem("Open Union UI", callback=self._on_open_ui),
            None,
            rumps.MenuItem("Quit", callback=rumps.quit_application),
        ]
        self._build_window_menu()

        self._kick_refresh()
        self._refresh_timer = rumps.Timer(self._on_timer, REFRESH_SECONDS)
        self._refresh_timer.start()
        self._render_timer = rumps.Timer(self._on_render_tick, 0.5)
        self._render_timer.start()

    # ---------- callbacks ----------

    def _on_timer(self, _sender) -> None:
        self._kick_refresh()

    def _on_render_tick(self, _sender) -> None:
        with self._lock:
            pending = self._pending_render
            self._pending_render = False
        if pending:
            self._render()

    def _on_refresh_click(self, _sender) -> None:
        self.title = "⏳ Union"
        self._kick_refresh()

    def _on_open_ui(self, _sender) -> None:
        if not self.host:
            return
        if self.filters:
            p, d = sorted(self.filters)[0]
            webbrowser.open(f"https://{self.host}/v2/domain/{d}/project/{p}/")
        else:
            webbrowser.open(f"https://{self.host}/v2/")

    def _on_toggle_filter(self, sender) -> None:
        pair = getattr(sender, "_pair", None)
        if pair is None:
            return
        if pair in self.filters:
            self.filters.remove(pair)
            sender.state = 0
        else:
            self.filters.add(pair)
            sender.state = 1
        _save_config(sorted(self.filters), self.window_label)
        self.title = "⏳ Union"
        self._kick_refresh()

    def _on_pick_window(self, sender) -> None:
        label = getattr(sender, "_label", None)
        if not label:
            return
        self.window_label = label
        for key in self._window_menu.keys():
            self._window_menu[key].state = 1 if key == label else 0
        _save_config(sorted(self.filters), self.window_label)
        # Filtering is applied at render time; no need to refetch.
        with self._lock:
            self._pending_render = True

    # ---------- refresh ----------

    def _kick_refresh(self) -> None:
        threading.Thread(target=self._refresh, daemon=True).start()

    def _refresh(self) -> None:
        try:
            if not self._flyte_ready:
                flyte.init_from_config()
                self._flyte_ready = True
                from flyte._initialize import get_client

                endpoint = get_client().endpoint or ""
                self.host = urlparse(endpoint).netloc or endpoint

            projects = list(Project.listall())
            available: list[tuple[str, str]] = []
            for p in projects:
                pdata = p.to_dict()
                pid = pdata.get("id") or pdata.get("name")
                domains = pdata.get("domains") or [{"id": "development"}]
                for d in domains:
                    did = d.get("id") or d.get("name")
                    if pid and did:
                        available.append((pid, did))
            available.sort()

            rows: list[RunRow] = []
            for pair in available:
                if pair not in self.filters:
                    continue
                p, d = pair
                try:
                    for r in Run.listall(
                        project=p,
                        domain=d,
                        limit=RECENT_LIMIT_PER_DOMAIN,
                        sort_by=("created_at", "desc"),
                    ):
                        started, ended = _parse_times(r)
                        rows.append(
                            RunRow(
                                project=p,
                                domain=d,
                                name=r.name,
                                phase=r.phase,
                                started=started,
                                ended=ended,
                                url=r.url,
                                task=_task_name(r),
                            )
                        )
                except Exception:
                    continue

            rows.sort(
                key=lambda x: x.started or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )

            # For the project picker: last-activity timestamp per pair. For
            # selected pairs we already have the full run list; for the rest
            # we probe a single most-recent run in parallel.
            last_activity: dict[tuple[str, str], datetime] = {}
            for row in rows:
                pair = (row.project, row.domain)
                ts = row.last_activity()
                if ts and (pair not in last_activity or ts > last_activity[pair]):
                    last_activity[pair] = ts

            to_probe = [p for p in available if p not in self.filters]

            def _probe(pair: tuple[str, str]):
                p, d = pair
                try:
                    for r in Run.listall(
                        project=p,
                        domain=d,
                        limit=1,
                        sort_by=("created_at", "desc"),
                    ):
                        st, en = _parse_times(r)
                        return pair, en or st
                except Exception:
                    pass
                return pair, None

            if to_probe:
                with ThreadPoolExecutor(max_workers=8) as pool:
                    for pair, ts in pool.map(_probe, to_probe):
                        if ts is not None:
                            last_activity[pair] = ts

            with self._lock:
                self.runs = rows
                self.available_pairs = available
                self.last_activity = last_activity
                self.error = None
                self.last_refresh = datetime.now(timezone.utc)
                self._pending_render = True
        except Exception as exc:  # noqa: BLE001
            with self._lock:
                self.error = f"{type(exc).__name__}: {exc}"
                self.last_refresh = datetime.now(timezone.utc)
                self._pending_render = True

    # ---------- render ----------

    def _render(self) -> None:
        with self._lock:
            all_runs = list(self.runs)
            available = list(self.available_pairs)
            last_activity = dict(self.last_activity)
            error = self.error
            last_refresh = self.last_refresh

        static = {"Projects", "Time window", "Refresh now", "Open Union UI", "Quit"}
        for key in list(self.menu.keys()):
            if key not in static:
                del self.menu[key]

        self._rebuild_projects_menu(available, last_activity)

        if error:
            self.title = "⚠️ Union"
            self.menu.insert_before(
                "Projects", rumps.MenuItem(f"Error: {error[:80]}")
            )
            self.menu.insert_before("Projects", None)
            return

        # Time-window filter. A run qualifies if it is currently running, or if
        # it started or ended within the window.
        hours = _window_hours(self.window_label)
        if hours is None:
            runs = all_runs
        else:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
            runs = [
                r
                for r in all_runs
                if r.is_running
                or (r.started and r.started >= cutoff)
                or (r.ended and r.ended >= cutoff)
            ]

        running = sum(1 for r in runs if r.is_running)
        terminal_runs = [r for r in runs if r.phase in TERMINAL_PHASES]
        failed = sum(
            1 for r in terminal_runs[:20] if r.phase != ActionPhase.SUCCEEDED
        )
        if running:
            icon_phase = ActionPhase.RUNNING
        elif terminal_runs:
            latest_finished = max(
                terminal_runs,
                key=lambda r: r.ended
                or r.started
                or datetime.min.replace(tzinfo=timezone.utc),
            )
            icon_phase = latest_finished.phase
        else:
            icon_phase = None
        self._set_status_title(icon_phase, "Union")

        if not self.filters:
            self.menu.insert_before(
                "Projects",
                rumps.MenuItem("No projects selected — pick one under Projects"),
            )
        elif not runs:
            self.menu.insert_before(
                "Projects",
                rumps.MenuItem(f"No runs in {self.window_label.lower()}"),
            )
        else:
            self._render_groups(runs)

        self.menu.insert_before("Projects", None)

        # Footer: just the last-refresh timestamp, rendered above Quit.
        if last_refresh:
            self.menu.insert_before("Quit", None)
            self.menu.insert_before(
                "Quit",
                rumps.MenuItem(f"Updated {_humanize_age(last_refresh)}"),
            )

    def _render_groups(self, runs: list[RunRow]) -> None:
        # Group by task/function name (falls back to the run id if missing).
        groups: dict[tuple[str, str, str], list[RunRow]] = {}
        for r in runs:
            key = (r.project, r.domain, r.task or r.name)
            groups.setdefault(key, []).append(r)

        def _latest(rs: list[RunRow]) -> datetime:
            return max(
                (r.last_activity() or datetime.min.replace(tzinfo=timezone.utc))
                for r in rs
            )

        ordered = sorted(groups.items(), key=lambda kv: _latest(kv[1]), reverse=True)

        for (project, domain, task), rs in ordered[:TOTAL_SHOWN]:
            rs.sort(
                key=lambda r: r.last_activity()
                or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )
            latest = rs[0]
            # Dot strip: oldest → newest, left → right (so the tip is the
            # most recent run, matching the Union UI).
            recent = list(reversed(rs[:DOT_STRIP_MAX]))
            recent_phases = [r.phase for r in recent]

            # Plain-text fallback label (used if attributed title fails).
            plain = f"{task}   " + " ".join(DOT_CHAR for _ in recent)
            group_item = rumps.MenuItem(plain)
            group_item.set_callback(self._make_opener(latest.url))
            group_item._menuitem.setAttributedTitle_(
                _build_group_title(task, recent_phases)
            )

            # Expandable submenu with each run in the group.
            for r in rs:
                sub_plain = (
                    f"{DOT_CHAR}  {r.name}  {_humanize_age(r.last_activity())}"
                )
                sub_item = rumps.MenuItem(
                    sub_plain, callback=self._make_opener(r.url)
                )
                sub_attr = NSMutableAttributedString.alloc().init()
                sub_attr.appendAttributedString_(
                    _attr(DOT_CHAR + "  ", color=PHASE_COLOR.get(r.phase))
                )
                sub_attr.appendAttributedString_(
                    _attr(f"{r.name}  {_humanize_age(r.last_activity())}")
                )
                sub_item._menuitem.setAttributedTitle_(sub_attr)
                group_item.add(sub_item)

            self.menu.insert_before("Projects", group_item)

    def _build_window_menu(self) -> None:
        for label, _ in TIME_WINDOWS:
            item = rumps.MenuItem(label, callback=self._on_pick_window)
            item._label = label
            item.state = 1 if label == self.window_label else 0
            self._window_menu[label] = item

    def _rebuild_projects_menu(
        self,
        available: list[tuple[str, str]],
        last_activity: dict[tuple[str, str], datetime],
    ) -> None:
        for key in list(self._projects_menu.keys()):
            del self._projects_menu[key]

        keys = sorted(
            set(available) | self.filters,
            key=lambda pair: (
                last_activity.get(pair) or datetime.min.replace(tzinfo=timezone.utc),
                pair,
            ),
            reverse=True,
        )
        for pair in keys:
            p, d = pair
            age = last_activity.get(pair)
            suffix = f"    {_humanize_age(age)}" if age else "    —"
            label = f"{p}/{d}{suffix}"
            item = rumps.MenuItem(label, callback=self._on_toggle_filter)
            item._pair = pair
            item.state = 1 if pair in self.filters else 0
            self._projects_menu[label] = item

    def _set_status_title(self, icon_phase, text: str) -> None:
        # Clear the text title and show the Union logo with an overlay dot.
        self.title = ""
        try:
            delegate = NSApplication.sharedApplication().delegate()
            nsitem = delegate.nsstatusitem
            color = PHASE_COLOR.get(icon_phase) if icon_phase is not None else None
            img = build_union_icon(height_pt=18.0, phase_color=color)
            nsitem.setImage_(img)
            nsitem.setTitle_("")
            nsitem.setToolTip_(text)
        except Exception:
            # Fall back to a text-only title so we at least see *something*.
            self.title = f"{DOT_CHAR if icon_phase else '○'} {text}"

    def _make_opener(self, url: str):
        def _cb(_sender):
            webbrowser.open(url)

        return _cb


def main() -> None:
    UnionStatusApp().run()


if __name__ == "__main__":
    main()
