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

import asyncio
import re

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
    NSMutableParagraphStyle,
    NSParagraphStyleAttributeName,
    NSRightTextAlignment,
    NSTextTab,
)
from Foundation import (
    NSAffineTransform,
    NSAttributedString,
    NSMakeRect,
    NSMutableAttributedString,
    NSSize,
)
from flyte.models import ActionPhase
from flyte.remote import Action, App, Project, Run


REFRESH_SECONDS = 60
RECENT_LIMIT_PER_DOMAIN = 100
TOTAL_SHOWN = 15
SUBMENU_RUN_CAP = 20  # max runs shown inside one group's submenu
SUMMARY_ACTIONS = 8  # tasks shown in a per-run hover tooltip
CONFIG_PATH = Path.home() / ".config" / "union-status" / "config.json"


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


# Right-aligned tab stop for the per-group label. Wide enough to comfortably
# fit task name + 10 dots in the menu.
GROUP_RIGHT_TAB_LOCATION = 320.0


def _right_aligned_para_style() -> NSMutableParagraphStyle:
    para = NSMutableParagraphStyle.alloc().init()
    tab = NSTextTab.alloc().initWithTextAlignment_location_options_(
        NSRightTextAlignment, GROUP_RIGHT_TAB_LOCATION, {}
    )
    para.setTabStops_([tab])
    return para


def _build_group_title(task: str, phases: list[ActionPhase]) -> NSAttributedString:
    """Render `{task}\\t●●●…` with dots in their phase colors, right-aligned."""
    para = _right_aligned_para_style()
    out = NSMutableAttributedString.alloc().init()
    out.appendAttributedString_(
        NSAttributedString.alloc().initWithString_attributes_(
            f"{task}\t", {NSParagraphStyleAttributeName: para}
        )
    )
    dot_font = NSFont.menuFontOfSize_(0)
    for i, p in enumerate(phases):
        color = PHASE_COLOR.get(p)
        attrs = {
            NSParagraphStyleAttributeName: para,
            NSForegroundColorAttributeName: color or NSColor.labelColor(),
            "NSFont": dot_font,
        }
        out.appendAttributedString_(
            NSAttributedString.alloc().initWithString_attributes_(
                DOT_CHAR, attrs
            )
        )
        if i < len(phases) - 1:
            out.appendAttributedString_(
                NSAttributedString.alloc().initWithString_attributes_(
                    " ", {NSParagraphStyleAttributeName: para}
                )
            )
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
    def is_running(self) -> bool:
        return self.phase not in TERMINAL_PHASES

    def last_activity(self) -> Optional[datetime]:
        if self.is_running:
            return datetime.now(timezone.utc)
        return self.ended or self.started


@dataclass
class AppRow:
    project: str
    domain: str
    name: str
    endpoint: str
    console_url: str


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


@dataclass
class ActionLite:
    """Lightweight view of a Flyte v2 Action — only the fields we render."""

    name: str
    task_name: str
    phase: ActionPhase
    start_time: Optional[datetime]


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


def _load_config() -> tuple[Optional[str], Optional[str], str]:
    """Return (project_override, domain_override, window_label).

    An explicit project/domain in config.json takes precedence over the
    union CLI's `task.project`/`task.domain`. If the user never picks one
    from the menu, both are None and we fall back to the union config.
    """
    window_labels = {label for label, _ in TIME_WINDOWS}
    try:
        data = json.loads(CONFIG_PATH.read_text())
    except Exception:
        return None, None, DEFAULT_WINDOW_LABEL
    window = data.get("window")
    if window not in window_labels:
        window = DEFAULT_WINDOW_LABEL
    return data.get("project"), data.get("domain"), window


def _save_config(
    project: Optional[str], domain: Optional[str], window_label: str
) -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload: dict = {"window": window_label}
    if project and domain:
        payload["project"] = project
        payload["domain"] = domain
    CONFIG_PATH.write_text(json.dumps(payload, indent=2))


def _window_hours(label: str) -> Optional[float]:
    for l, h in TIME_WINDOWS:
        if l == label:
            return h
    return None


class UnionStatusApp(rumps.App):
    def __init__(self) -> None:
        super().__init__("Union", title="⚪ Union", quit_button=None)
        self.host: str = ""
        # Scope: if the user has picked one via the Projects menu, that is
        # persisted in config.json and overrides ~/.union/config.yaml. If
        # not, we fall back to the union CLI's task.project/task.domain.
        saved_project, saved_domain, window = _load_config()
        self.project: Optional[str] = saved_project
        self.domain: Optional[str] = saved_domain
        self.runs: list[RunRow] = []
        self.apps: list[AppRow] = []
        self.available_pairs: list[tuple[str, str]] = []
        self.last_activity: dict[tuple[str, str], datetime] = {}
        # Per-run recent-task summary, keyed by (project, domain, run_name).
        self.summary_cache: dict[tuple[str, str, str], list[ActionLite]] = {}
        self.window_label: str = window
        self.error: Optional[str] = None
        self.last_refresh: Optional[datetime] = None
        self._lock = threading.Lock()
        self._pending_render = False
        self._flyte_ready = False

        self._projects_menu = rumps.MenuItem("Project")
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
        if self.project and self.domain:
            webbrowser.open(
                f"https://{self.host}/v2/domain/{self.domain}/project/{self.project}/"
            )
        else:
            webbrowser.open(f"https://{self.host}/v2/")

    def _on_pick_window(self, sender) -> None:
        label = getattr(sender, "_label", None)
        if not label:
            return
        self.window_label = label
        for key in self._window_menu.keys():
            self._window_menu[key].state = 1 if key == label else 0
        _save_config(self.project, self.domain, self.window_label)
        # Time-window filtering is applied at render time; no refetch needed.
        with self._lock:
            self._pending_render = True

    def _on_pick_project(self, sender) -> None:
        pair = getattr(sender, "_pair", None)
        if pair is None:
            return
        new_project, new_domain = pair
        if new_project == self.project and new_domain == self.domain:
            return
        self.project = new_project
        self.domain = new_domain
        _save_config(self.project, self.domain, self.window_label)
        # Force re-init on the next refresh (happens on the worker thread so
        # the UI stays responsive while auth/networking runs).
        with self._lock:
            self._flyte_ready = False
            self.runs = []
            self.apps = []
        self.title = "⏳ Union"
        self._kick_refresh()

    # ---------- refresh ----------

    def _kick_refresh(self) -> None:
        threading.Thread(target=self._refresh, daemon=True).start()

    def _refresh(self) -> None:
        try:
            if not self._flyte_ready:
                # Passing project/domain overrides the union-config defaults.
                # If both are None, init_from_config reads task.project and
                # task.domain from ~/.union/config.yaml.
                flyte.init_from_config(
                    project=self.project, domain=self.domain
                )
                self._flyte_ready = True
                from flyte._initialize import get_client, get_init_config

                endpoint = get_client().endpoint or ""
                self.host = urlparse(endpoint).netloc or endpoint
                cfg = get_init_config()
                # Adopt the resolved values so the menu's Showing: line and
                # the Project picker reflect what Flyte actually ended up
                # scoped to (important on first launch when the user hasn't
                # picked yet and we're using yaml defaults).
                self.project = cfg.project
                self.domain = cfg.domain

            # Cluster-wide project list, for the Project picker. Cheap; one
            # paginated call. Failures here shouldn't block runs/apps.
            available: list[tuple[str, str]] = []
            try:
                for p in Project.listall():
                    pdata = p.to_dict()
                    pid = pdata.get("id") or pdata.get("name")
                    domains = pdata.get("domains") or [{"id": "development"}]
                    for d in domains:
                        did = d.get("id") or d.get("name")
                        if pid and did:
                            available.append((pid, did))
                available.sort()
            except Exception:
                pass

            if not self.project or not self.domain:
                with self._lock:
                    self.runs = []
                    self.apps = []
                    self.available_pairs = available
                    self.error = (
                        "No project/domain configured — pick one under Project"
                        if available
                        else "No project/domain in ~/.union/config.yaml "
                        "(set task.project and task.domain)"
                    )
                    self.last_refresh = datetime.now(timezone.utc)
                    self._pending_render = True
                return

            rows: list[RunRow] = []
            for r in Run.listall(
                limit=RECENT_LIMIT_PER_DOMAIN,
                sort_by=("created_at", "desc"),
            ):
                started, ended = _parse_times(r)
                rows.append(
                    RunRow(
                        project=self.project,
                        domain=self.domain,
                        name=r.name,
                        phase=r.phase,
                        started=started,
                        ended=ended,
                        url=r.url,
                        task=_task_name(r),
                    )
                )

            rows.sort(
                key=lambda x: x.started or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )

            # Active apps in the current project/domain. App.listall is already
            # scoped by the init'd cfg.project/cfg.domain, so no extra filter
            # is needed.
            apps: list[AppRow] = []
            for a in App.listall(limit=200):
                if not a.is_active():
                    continue
                apps.append(
                    AppRow(
                        project=a.pb2.metadata.id.project,
                        domain=a.pb2.metadata.id.domain,
                        name=a.name,
                        endpoint=a.endpoint,
                        console_url=a.url,
                    )
                )

            with self._lock:
                self.runs = rows
                self.apps = apps
                self.available_pairs = available
                self.error = None
                self.last_refresh = datetime.now(timezone.utc)
                self._pending_render = True

            # Lazily fetch per-run task summaries for the runs the menu will
            # actually display. Runs in progress are always re-fetched; finished
            # runs are cached for the life of the process.
            self._refresh_run_summaries(rows)
        except Exception as exc:  # noqa: BLE001
            with self._lock:
                self.error = f"{type(exc).__name__}: {exc}"
                self.last_refresh = datetime.now(timezone.utc)
                self._pending_render = True

    def _refresh_run_summaries(self, rows: list[RunRow]) -> None:
        # Mirror the menu's grouping/cap so we only fetch what the user can see.
        groups: dict[tuple[str, str, str], list[RunRow]] = {}
        for r in rows:
            key = (r.project, r.domain, r.task or r.name)
            groups.setdefault(key, []).append(r)

        visible: list[RunRow] = []
        for rs in groups.values():
            rs.sort(
                key=lambda r: r.last_activity()
                or datetime.min.replace(tzinfo=timezone.utc),
                reverse=True,
            )
            visible.extend(rs[:SUBMENU_RUN_CAP])

        with self._lock:
            cached = set(self.summary_cache.keys())

        to_fetch = [
            r
            for r in visible
            if r.is_running or (r.project, r.domain, r.name) not in cached
        ]
        if not to_fetch:
            return

        async def _summary_of(
            row: RunRow,
        ) -> tuple[RunRow, Optional[list[ActionLite]]]:
            try:
                actions: list[ActionLite] = []
                i = 0
                async for a in Action.listall.aio(
                    for_run_name=row.name,
                    sort_by=("created_at", "desc"),
                ):
                    if i >= SUMMARY_ACTIONS:
                        break
                    actions.append(
                        ActionLite(
                            name=a.name,
                            task_name=getattr(a, "task_name", None) or a.name,
                            phase=a.phase,
                            start_time=getattr(a, "start_time", None),
                        )
                    )
                    i += 1
                return row, actions or None
            except Exception:
                return row, None

        async def _fetch_all() -> list[tuple[RunRow, Optional[list[ActionLite]]]]:
            return await asyncio.gather(*(_summary_of(r) for r in to_fetch))

        # Run the fan-out on a fresh loop. We're in a worker thread here (the
        # rumps/Cocoa main thread has its own event loop that we mustn't touch),
        # so asyncio.run is safe: it creates + tears down its own loop.
        results = asyncio.run(_fetch_all())

        new: dict[tuple[str, str, str], list[ActionLite]] = {}
        for row, summary in results:
            if summary:
                new[(row.project, row.domain, row.name)] = summary

        if not new:
            return

        with self._lock:
            self.summary_cache.update(new)
            self._pending_render = True

    # ---------- render ----------

    def _render(self) -> None:
        with self._lock:
            all_runs = list(self.runs)
            all_apps = list(self.apps)
            available = list(self.available_pairs)
            error = self.error
            last_refresh = self.last_refresh

        static = {"Project", "Time window", "Refresh now", "Open Union UI", "Quit"}
        for key in list(self.menu.keys()):
            if key not in static:
                del self.menu[key]

        self._build_projects_menu(available)

        # Header (top of menu): which project/domain we're showing. Inserted
        # first so it ends up above everything else. insert_before places
        # items right before the anchor in FIFO order, so the first call is
        # farthest from the anchor.
        if self.project and self.domain:
            self.menu.insert_before(
                "Project",
                rumps.MenuItem(f"Showing: {self.project}/{self.domain}"),
            )
            self.menu.insert_before("Project", None)

        if error:
            self.title = "⚠️ Union"
            self.menu.insert_before(
                "Project", rumps.MenuItem(f"Error: {error[:100]}")
            )
            self.menu.insert_before("Project", None)
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

        if not runs:
            self.menu.insert_before(
                "Project",
                rumps.MenuItem(f"No runs in {self.window_label.lower()}"),
            )
        else:
            self._render_groups(runs)

        if all_apps:
            self.menu.insert_before("Project", None)
            self.menu.insert_before(
                "Project", rumps.MenuItem("Active apps")
            )
            for a in sorted(all_apps, key=lambda x: x.name):
                self._render_app_row(a)

        self.menu.insert_before("Project", None)

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

            # Each run is its own hover-expandable submenu listing the most
            # recent tasks inside that run (mirrors the Projects/Time window
            # styling, with an arrow indicator).
            for r in rs[:SUBMENU_RUN_CAP]:
                sub_plain = (
                    f"{DOT_CHAR}  {r.name}  {_humanize_age(r.last_activity())}"
                )
                # Clicking the run row opens its Union page (same behavior as
                # the top-level group row); hovering reveals the task list.
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

                actions = self.summary_cache.get((r.project, r.domain, r.name))
                if actions:
                    for a in actions:
                        when = (
                            _humanize_age(a.start_time) if a.start_time else ""
                        )
                        task_plain = f"{DOT_CHAR}  {a.task_name}   {when}"
                        task_url = f"{r.url}?i={a.name}"
                        task_item = rumps.MenuItem(
                            task_plain, callback=self._make_opener(task_url)
                        )
                        task_attr = NSMutableAttributedString.alloc().init()
                        task_attr.appendAttributedString_(
                            _attr(
                                DOT_CHAR + "  ",
                                color=PHASE_COLOR.get(a.phase),
                            )
                        )
                        suffix = f"  {when}" if when else ""
                        task_attr.appendAttributedString_(
                            _attr(f"{a.task_name}{suffix}")
                        )
                        task_item._menuitem.setAttributedTitle_(task_attr)
                        sub_item.add(task_item)
                else:
                    sub_item.add(rumps.MenuItem("Loading task list…"))
                sub_item._menuitem.setAttributedTitle_(sub_attr)
                group_item.add(sub_item)

            self.menu.insert_before("Project", group_item)

    def _render_app_row(self, a: AppRow) -> None:
        # Primary click opens the exposed endpoint; a nested submenu row
        # opens the Union console page for inspecting the app.
        plain = f"{DOT_CHAR}  {a.name}"
        item = rumps.MenuItem(plain, callback=self._make_opener(a.endpoint))
        attr = NSMutableAttributedString.alloc().init()
        attr.appendAttributedString_(
            _attr(DOT_CHAR + "  ", color=PHASE_COLOR[ActionPhase.RUNNING])
        )
        attr.appendAttributedString_(_attr(a.name))
        item._menuitem.setAttributedTitle_(attr)
        item.add(
            rumps.MenuItem(
                "Open in Union console",
                callback=self._make_opener(a.console_url),
            )
        )
        self.menu.insert_before("Project", item)

    def _build_window_menu(self) -> None:
        for label, _ in TIME_WINDOWS:
            item = rumps.MenuItem(label, callback=self._on_pick_window)
            item._label = label
            item.state = 1 if label == self.window_label else 0
            self._window_menu[label] = item

    def _build_projects_menu(
        self, available: list[tuple[str, str]]
    ) -> None:
        for key in list(self._projects_menu.keys()):
            del self._projects_menu[key]
        # Include the current pick even if the cluster listing failed or
        # hasn't returned yet, so the radio state is always correct.
        pairs = sorted(set(available) | {
            (self.project, self.domain) if self.project and self.domain else None
        } - {None})
        if not pairs:
            placeholder = rumps.MenuItem("Loading…")
            self._projects_menu["Loading…"] = placeholder
            return
        for pair in pairs:
            p, d = pair
            label = f"{p}/{d}"
            item = rumps.MenuItem(label, callback=self._on_pick_project)
            item._pair = pair
            item.state = 1 if pair == (self.project, self.domain) else 0
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
