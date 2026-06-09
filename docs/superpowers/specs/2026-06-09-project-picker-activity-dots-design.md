# Project picker activity dots — design

**Date:** 2026-06-09
**Status:** Approved (brainstorming)

## Goal

In the **Project** picker menu, show a status icon next to each project that
reflects the project's **most recent activity within the selected time
window**. The dot reuses the app's existing phase colors; idle projects (no
activity in the window) get a dim hollow `○`.

## Decisions

1. **Icon meaning — phase of the latest run.** Reuse `PHASE_COLOR`
   (green = succeeded, blue = running, red = failed, orange = aborted,
   purple = queued). A project with no activity in the window renders a dim
   hollow `○`.
2. **Scope — any environment.** A project's dot reflects its single most
   recent run across *all* of its domains, so the dot does not change when the
   user switches the Environment menu.
3. **Idle projects — keep all, dim the idle ones.** Every project stays listed
   and selectable; idle ones just get the hollow `○`. No project is hidden.
4. **Time window — the existing Time window menu** (`self.window_label` →
   hours). "Ever" means any run ever counts as active.

## Approach (chosen)

**Per-pair most-recent-run fan-out, window-filtered at render time.**

On each refresh we already build `self.available_pairs` (every
`(project, domain)` in the cluster). After that, fan out one lightweight
`Run.listall(project=p, domain=d, sort_by=("created_at","desc"), limit=1)`
per pair via `asyncio.gather` — the same fan-out pattern already used in
`_refresh_run_summaries`. Each result is reduced to a small record of the
run's phase and its activity timestamp, cached in `self.last_activity`.

The **time-window filter is applied at render time**, not at fetch time. This
matches the existing design: `_on_pick_window` only sets `_pending_render`
(no refetch), and runs are likewise window-filtered in `_render`. So switching
the Time window re-colors the dots instantly without new network calls.

### Approaches rejected

- **Lazy fetch on menu-open with TTL cache.** Lower steady-state cost, but
  rumps offers no clean "menu will open" hook and it adds cache-invalidation
  complexity.
- **Derive from already-fetched data.** Impossible — the app only fetches runs
  for the *selected* project/domain, so there is no data for other projects.

## Components

### 1. Data model

```python
@dataclass
class ProjectActivity:
    phase: ActionPhase
    last_activity: Optional[datetime]  # running -> now(), else ended or started
```

`self.last_activity: dict[tuple[str, str], ProjectActivity]` — keyed by
`(project, domain)`. (Repurposes the existing dead stub at `main.py:545`,
which is currently declared but never read or written.)

### 2. Fetch (worker thread, in `_refresh`)

After `available_pairs` is built and the main runs/apps fetch completes:

- Fan out `Run.listall(project=p, domain=d, sort_by=("created_at","desc"),
  limit=1)` across all pairs with `asyncio.gather`, mirroring
  `_refresh_run_summaries` (its own event loop via `asyncio.run`, since we are
  on a worker thread).
- For the **currently-selected pair**, skip the extra query and derive the
  record from `self.runs` we already fetched (newest run's phase +
  `last_activity()`), to avoid a redundant call.
- Reduce each pair's most-recent run to `ProjectActivity(phase,
  last_activity)`, where `last_activity` follows `RunRow.last_activity()`
  semantics (a running run → `now()`).
- Each pair query is wrapped in `try/except`; a failure means that pair is
  simply absent (→ idle dot) and never blocks the menu. Results only
  **overwrite** prior cached values on success, so a transient network blip
  does not blank existing dots.
- Store the merged result under `self._lock` and set `_pending_render`.

**Known approximation:** sorting by `created_at desc` + `limit=1` returns the
most recently *created* run, which is a cheap stand-in for "most recent
activity." A long-running run created earlier but still active could in
principle be missed in favor of a newer, already-finished run. This is
acceptable for a status dot; if it proves misleading in practice, bump to
`limit=3–5` and take the entry with the max `last_activity()`. Prefer
`sort_by=("updated_at","desc")` if that sort field is supported (verify at
implementation time — the main fetch uses `created_at`).

**Implementation-time optimization to verify:** check whether
`Run.listall(project=p)` with `domain` unset lists runs across *all* domains
of the project. If it does, collapse the fan-out to **one query per project**
instead of one per `(project, domain)` pair, cutting cost substantially. If it
does not (or silently scopes to the init'd domain), keep the per-pair fan-out.

### 3. Aggregation (pure, testable helper)

```python
def _project_dot_phase(
    entries: list[ProjectActivity],
    window_hours: Optional[float],
    now: datetime,
) -> Optional[ActionPhase]:
    """Phase to color the project's dot, or None if idle in the window.

    Picks the entry with the newest last_activity. Returns its phase if that
    timestamp is within window_hours of `now` (or window_hours is None =
    "Ever"); otherwise None.
    """
```

Kept free of Cocoa/flyte so it is unit-testable. `now` is injected for
deterministic tests.

### 4. Render (`_build_projects_menu`)

For each project, aggregate its domains' `ProjectActivity` entries (from the
snapshot) and call `_project_dot_phase`:

- **Phase returned** → prepend a colored `●` using the existing
  `_attr(...)` + `setAttributedTitle_` pattern with `PHASE_COLOR[phase]`.
- **None (idle)** → prepend a dim hollow `○` in `secondaryLabelColor()`.

The radio checkmark (`item.state`) is unchanged — it lives in the menu item's
state gutter, independent of the (attributed) title — so the selected project
still shows its checkmark alongside the dot. A plain-text fallback title
(dot char + name) is set first, then the attributed title, matching how
`_render_groups` / `_render_app_row` already do it.

### 5. Threading / render integration

`_render` already snapshots shared state under `self._lock` (main.py:861–866).
Add `last_activity` to that snapshot and pass it into `_build_projects_menu`
(its signature gains the activity map). All Cocoa work stays on the main
thread; all fetching stays on the worker thread.

## Data flow

```
worker thread (_refresh):
  Project.listall ----------------> available_pairs
  per pair: Run.listall(limit=1) --> {(proj,dom): ProjectActivity}  --\
  selected pair: from self.runs ---------------------------------------+--> self.last_activity (under lock)
                                                                            set _pending_render

main thread (_render -> _build_projects_menu):
  snapshot last_activity + window_label
  per project: group entries by project
               _project_dot_phase(entries, window_hours, now)
               -> colored ● (PHASE_COLOR) or dim ○
```

## Error handling

- Per-pair query failure → pair omitted → idle `○`; never raised to the menu.
- Whole fan-out failure → caught like the rest of `_refresh`; dots fall back to
  whatever was last cached (or hollow), menu still renders.
- Before activity data has arrived (first paint, or listing still in flight),
  projects render with the dim `○` — no crash, no blocking.

## Testing

Add a focused unit test for the pure helper `_project_dot_phase`:

- newest-wins: among several entries, the most recent `last_activity` decides
  the phase.
- window boundary: an entry just inside vs. just outside `window_hours`.
- running-counts-as-now: a `RUNNING` entry always falls in any window.
- "Ever" window (`window_hours=None`) → any non-empty entry is active.
- empty entries → `None` (idle).

(The repo currently has no committed tests — only stale `.pyc` artifacts under
`tests/__pycache__/` — so this adds the first tracked test alongside the new
pure helper.)

## Out of scope

- Activity dots on the **Environment** menu. The per-pair data would make this
  an easy follow-on (per-domain dots), but this change is limited to the
  Project picker as requested.
