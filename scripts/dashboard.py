# /// script
# requires-python = ">=3.10"
# dependencies = ["textual>=1.0"]
# ///
"""dual-author dashboard — a Textual TUI (the Python equivalent of ratatui).

Launched by `monitor.py watch` via `uv run` (uv provisions python+textual in a
cached env on first use; pass --legacy to monitor.py for the plain-text render).
All data retrieval — herdr/gh polling, workspace/agent/tab renames, reviewer
sweeps — runs in a SEPARATE PROCESS (`monitor.py collect`, spawned here) that
writes an atomic JSON snapshot every 5s; the UI only re-reads that file, so it
is never blocked by slow herdr/gh calls. Textual owns layout, clipping, and
keys, so the dashboard always fits its pane.

Views
  main   header (repo · running/queued/done · clock) · issues table |
         detail panel (PR + checks, review rounds with per-slot verdicts,
         live tail of the worker's pane) · activity feed · key footer
  graph  [g] the blocking DAG between this run's issues (GitHub issue
         dependencies + "blocked by #N" body conventions; queued issues
         included, external blockers flagged), above the full pipeline chain
         per issue — implement → draft PR → review rounds → checks → merge

Keys   ↑/↓ j/k select · Enter/f focus worker pane in herdr · g graph ·
       o open PR · r poll now · p cycle poll interval (5/20/60s, persisted) ·
       q quit (pipeline keeps running)
"""
import json
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import monitor as M

from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.screen import Screen
from textual.widgets import DataTable, Footer, RichLog, Static

SPIN = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"


# ---- shared data ------------------------------------------------------------

class Collector:
    """Data source for the UI. The heavy polling (dozens of herdr/gh subprocess
    calls per tick) runs in a SEPARATE PROCESS — `monitor.py collect` — so the
    UI can never be blocked by data retrieval (a thread wasn't enough: the GIL
    is held during each subprocess spawn, which visibly starved the UI every
    tick). The UI just re-reads the small JSON snapshot the collector writes,
    parsing it only when its mtime changes."""

    def __init__(self, issues, queued):
        self.issues, self.queued = list(issues), list(queued)
        self.path = os.path.join(M.base(), "dashboard.json")
        self._mtime, self._snap = 0.0, {}
        self.proc = None

    def start(self):
        argv = [sys.executable,
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "monitor.py"),
                "collect"]
        if self.queued:
            argv += ["--queued", ",".join(self.queued)]
        argv += self.issues
        env = dict(os.environ, DUAL_AUTHOR_COLLECT_PPID=str(os.getpid()))
        self.proc = subprocess.Popen(argv, env=env, stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()

    def poll_now(self):
        try:
            os.makedirs(M.base(), exist_ok=True)
            open(os.path.join(M.base(), "poll-now"), "w").close()
        except OSError:
            pass

    def cycle_interval(self):
        """Advance the polling cadence through monitor.POLL_CHOICES (5/20/60s);
        the collector re-reads the file every second, so it applies mid-sleep."""
        cur = M.poll_interval()
        nxt = M.POLL_CHOICES[(M.POLL_CHOICES.index(cur) + 1) % len(M.POLL_CHOICES)]
        try:
            os.makedirs(M.base(), exist_ok=True)
            with open(os.path.join(M.base(), "poll-interval"), "w") as f:
                f.write(str(nxt))
        except OSError:
            pass
        return nxt

    def _read(self):
        try:
            m = os.stat(self.path).st_mtime
            if m != self._mtime:
                with open(self.path) as f:
                    self._snap = json.load(f)
                self._mtime = m
        except (OSError, ValueError):
            pass  # mid-replace or absent — keep the last good snapshot
        return self._snap

    def snap(self):
        s = self._read()
        err = s.get("err")
        stale_after = max(30, 2 * M.poll_interval() + 10)
        if s and time.time() - s.get("ts", 0) > stale_after:
            err = err or f"collector stale — no update for >{stale_after}s"
        if self.proc and self.proc.poll() is not None:
            err = f"collector exited ({self.proc.returncode})"
        return (s.get("rows") or [], s.get("queue") or [],
                s.get("state") or {}, err, 1 if s else 0)

    def deps(self):
        return self._read().get("qdeps") or {}

    def repo(self):
        return self._read().get("repo") or ""


def row_glyph(r):
    if r["verdict"]:
        return Text("✓", "bold green")
    if r["input"] or r["status"] == "blocked":
        return Text("!", "bold red")
    if r["status"] == "missing":
        return Text("✗", "red")
    if r["status"] == "working":
        return Text(SPIN[int(time.time() * 8) % len(SPIN)], "bold yellow")
    if r["status"] == "idle":
        return Text("·", "cyan")
    return Text("?", "dim")


VERDICT_STYLE = {"PASS": ("✓ pass", "green"), "FAIL": ("✗ FAIL", "bold red"),
                 "CANCELLED": ("– cancelled", "dim"), "running": ("… running", "yellow"),
                 "MISSING": ("? missing", "red"), "SPAWN-FAILED": ("? spawn-failed", "red")}


def checks_text(pr):
    ck = (pr or {}).get("checks") or {}
    t = Text()
    for k, g, style in (("ok", "✓", "green"), ("fail", "✗", "red"), ("pending", "◌", "yellow")):
        if ck.get(k):
            t.append(f"{ck[k]}{g}", style)
    return t


def diff_text(r):
    d = r.get("diff")
    if not d:
        return Text("—", "dim")
    t = Text()
    t.append(f"+{d.get('add', 0)}", "green")
    t.append(f" -{d.get('del', 0)}", "red")
    return t


def pr_str(pr):
    if not pr:
        return "—"
    n = f"#{pr.get('number')}"
    if pr.get("state") == "MERGED":
        return f"{n} merged"
    return f"{n} draft" if pr.get("draft") else f"{n} open"


def issue_times(state, n, now):
    st = state.get(str(n), {})
    end = st.get("done", now)
    total = M.fmt_dur(end - st["start"]) if "start" in st else "-"
    in_ph = "-" if "done" in st or "phase_start" not in st else M.fmt_dur(now - st["phase_start"])
    return in_ph, total


def stages(r):
    """The issue's pipeline as (label, state) steps; state in done|run|fail|todo.
    Derived from phase + on-disk review rounds + PR ground truth."""
    ph, pr, rounds = r["phase"], r.get("pr"), r.get("rounds") or []
    merged = bool(pr and pr.get("state") == "MERGED")
    out = [("implement", "run" if ph in ("starting", "implementing") and not r["verdict"] else "done"),
           (f"PR {pr_str(pr)}" if pr else "draft PR",
            "done" if pr else ("run" if ph == "pushing-pr" else "todo"))]
    for i, rd in enumerate(rounds):
        vs = rd["slots"]
        label = f"review {rd['tag']}  " + " · ".join(
            f"{s} {('…' if v == 'running' else v.lower())}" for s, v in sorted(vs.items()))
        if any(v == "running" for v in vs.values()):
            st = "run"
        elif any(v in ("FAIL", "MISSING", "SPAWN-FAILED") for v in vs.values()):
            st = "fail"
        else:
            st = "done"
        out.append((label, st))
        if st == "fail" and i < len(rounds) - 1:
            out.append((f"fix after {rd['tag']}", "done"))
    if ph.startswith("fixing-round"):
        out.append((ph, "run"))
    ckst = "todo"
    if pr:
        f = pr.get("checks") or {}
        ckst = "fail" if f.get("fail") else ("run" if f.get("pending") else ("done" if f.get("ok") else "todo"))
    ck = checks_text(pr).plain
    out.append((f"checks {ck}" if ck else "checks", "done" if merged else ckst))
    if merged:
        out.append(("merged", "done"))
    elif r["verdict"]:
        out.append(("auto-merge armed", "run"))
    else:
        out.append(("ready + merge", "todo"))
    return out


STAGE_MARK = {"done": ("✓", "green"), "run": ("▶", "bold yellow"),
              "fail": ("✗", "bold red"), "todo": (" ", "dim")}


def pipeline_text(r, state, selected=False):
    now = time.time()
    _, total = issue_times(state, r["issue"], now)
    t = Text()
    t.append(f"#{r['issue']}  {r['phase']}  ({total})\n",
             "bold reverse" if selected else "bold")
    for label, st in stages(r):
        mark, style = STAGE_MARK[st]
        t.append(f" [{mark}] ", style)
        t.append(label + "\n", style if st != "todo" else "dim")
    return t


def dag_text(rows, q, qdeps, state):
    """The blocking DAG across this run's issues (active + queued) as a tree:
    roots are unblocked issues; children are the issues they block. Blockers
    outside the run are noted inline. Cycle-safe."""
    now = time.time()
    ids = [r["issue"] for r in rows] + [n for n in q]
    inset = set(ids)
    byid = {r["issue"]: r for r in rows}
    blocked = {r["issue"]: [str(d) for d in (r.get("blocked_by") or [])] for r in rows}
    blocked.update({n: [str(d) for d in (qdeps.get(n) or [])] for n in q})
    # A DAG is not a tree: an issue with several blockers would be printed once per
    # blocker, repeating its whole subtree. Give every issue ONE place in the tree —
    # under the blocker that frees it LAST (its deepest one, which is when it can
    # actually start) — and name its other blockers inline on that line.
    inruns = {n: [d for d in blocked.get(n, []) if d in inset] for n in ids}

    def depth(n, stack=()):  # longest blocker chain above n; cycle-safe
        if n in stack:
            return 0
        return 1 + max((depth(d, stack + (n,)) for d in inruns.get(n, [])), default=-1)

    primary = {}
    for n in ids:
        ds = inruns.get(n) or []
        if ds:
            primary[n] = max(ds, key=lambda d: (depth(d), -ids.index(d)))
    children = {}
    for n, d in primary.items():
        children.setdefault(d, []).append(n)
    roots = [n for n in ids if not inruns.get(n)]
    t = Text()
    t.append("blocking DAG\n", "bold underline")
    if not any(children.values()):
        t.append("no blocking dependencies between this run's issues\n", "dim")
        return t

    def node_label(n):
        r = byid.get(n)
        lbl = Text()
        lbl.append(f"#{n}", "bold")
        if r:
            g = row_glyph(r)
            lbl.append("  ").append(g).append(f" {r['phase']}")
            _, total = issue_times(state, n, now)
            lbl.append(f"  ({total})", "dim")
        else:
            lbl.append("  ⏳ queued", "dim")
        others = [d for d in inruns.get(n, []) if d != primary.get(n)]
        if others:
            lbl.append("   also after " + " ".join(f"#{d}" for d in others), "cyan")
        ext = [d for d in blocked.get(n, []) if d not in inset]
        if ext:
            lbl.append("   ⛓ also blocked by " + " ".join(f"#{d}" for d in ext)
                       + " (outside this run)", "yellow")
        return lbl

    seen, printed = set(), set()

    def walk(n, prefix, is_last, is_root):
        if n in seen:
            t.append(prefix + ("└─ " if is_last else "├─ ") + f"#{n} (cycle)\n", "red")
            return
        seen.add(n)
        printed.add(n)
        if is_root:
            t.append(node_label(n)).append("\n")
            child_prefix = ""
        else:
            t.append(prefix + ("└─▶ " if is_last else "├─▶ "), "dim")
            t.append(node_label(n)).append("\n")
            child_prefix = prefix + ("    " if is_last else "│   ")
        kids = children.get(n, [])
        for i, k in enumerate(kids):
            walk(k, child_prefix, i == len(kids) - 1, False)
        seen.discard(n)

    for rt in roots:
        walk(rt, "", True, True)
    for n in ids:  # cycle-only components have no root — print them flat
        if n not in printed:
            walk(n, "", True, True)
    return t


# ---- widgets -----------------------------------------------------------------

class GraphScreen(Screen):
    """[g] — every issue's full pipeline chain in one scrollable grid."""
    BINDINGS = [Binding("g,escape", "app.pop_screen", "main view"),
                Binding("q", "app.quit", "quit"),
                Binding("r", "app.poll", "poll"),
                Binding("p", "app.cycle_poll", "interval")]

    def compose(self) -> ComposeResult:
        yield Static(id="gheader")
        with VerticalScroll(id="gbody"):
            yield Static(id="gdag")
            yield Container(id="gpipes")
        yield Static(id="gqueue")
        yield Footer()

    def on_mount(self):
        self.refresh_data()
        self.set_interval(1.0, self.refresh_data)

    def refresh_data(self):
        app = self.app
        rows, q, state, err, _ = app.collector.snap()
        self.query_one("#gheader", Static).update(app.header_text(rows, q, err))
        self.query_one("#gqueue", Static).update(
            Text("queued: " + "  ".join(f"#{n}" for n in q), "dim") if q else Text(""))
        self.query_one("#gdag", Static).update(
            dag_text(rows, q, app.collector.deps(), state))
        body = self.query_one("#gbody")
        pipes = self.query_one("#gpipes")
        blocks = [pipeline_text(r, state, i == app.sel_index(rows))
                  for i, r in enumerate(rows)]
        want = len(blocks)
        have = len(pipes.children)
        if want != have:  # mount/remove relayouts — pin the scroll position
            keep_y = body.scroll_y
            for i in range(have, want):
                pipes.mount(Static(classes="pipeline"))
            for extra in list(pipes.children)[want:]:
                extra.remove()
            self.call_after_refresh(lambda y=keep_y: body.scroll_to(y=y, animate=False))
        for w, t in zip(pipes.children, blocks):
            w.update(t)


class DualAuthorApp(App):
    TITLE = "dual-author"
    CSS = """
    #header { height: 1; background: $primary; color: $text; padding: 0 1; }
    #body { height: 1fr; }
    #issues { width: 2fr; }
    #detail { width: 1fr; border-left: solid $secondary; padding: 0 1; }
    #dhead { height: auto; }
    #rounds { height: auto; }
    #tail { height: 1fr; border-top: dashed $secondary; }
    #activity { dock: bottom; height: 5; border-top: dashed $secondary;
                padding: 0 1; color: $text-muted; }
    #gbody { padding: 0 1; }
    #gdag { height: auto; margin-bottom: 1; }
    #gpipes { layout: grid; grid-size: 2; grid-rows: auto; grid-gutter: 0 2; height: auto; }
    .pipeline { height: auto; margin-bottom: 1; }
    #gheader { height: 1; background: $primary; color: $text; padding: 0 1; }
    #gqueue { height: 1; padding: 0 1; }
    """
    BINDINGS = [Binding("q", "quit", "quit"),
                Binding("g", "graph", "graph"),
                Binding("f,enter", "focus_worker", "focus pane", priority=True),
                Binding("o", "open_pr", "open PR"),
                Binding("r", "poll", "poll"),
                Binding("p", "cycle_poll", "interval"),
                Binding("j", "cursor(1)", show=False),
                Binding("k", "cursor(-1)", show=False)]

    def __init__(self, issues=(), queued=()):
        super().__init__()
        self.collector = Collector(issues, queued)

    # -- layout
    def compose(self) -> ComposeResult:
        yield Static(id="header")
        with Horizontal(id="body"):
            yield DataTable(id="issues", cursor_type="row")
            with Vertical(id="detail"):
                yield Static(id="dhead")
                yield Static(id="rounds")
                yield RichLog(id="tail", markup=False, wrap=False, auto_scroll=True)
        yield Static(id="activity")
        yield Footer()

    def on_mount(self):
        t = self.query_one(DataTable)
        self._cols = t.add_columns("", "issue", "phase", "in-phase", "total", "+/-", "PR", "checks")
        self.collector.start()
        self.refresh_data()
        self.set_interval(1.0, self.refresh_data)

    # -- data → widgets
    def sel_index(self, rows):
        t = self.query_one(DataTable)
        return t.cursor_row if rows else 0

    def header_text(self, rows, q, err):
        running = sum(1 for r in rows if not r["verdict"] and r["status"] != "missing")
        done = sum(1 for r in rows if r["verdict"])
        blocked = sum(1 for r in rows if (r["input"] or r["status"] == "blocked") and not r["verdict"])
        repo = self.collector.repo() or M.ns()  # from the snapshot — no gh call here
        t = Text()
        t.append(f"dual-author  {repo}   ", "bold")
        t.append(f"●{running} running  ⏳{len(q)} queued  ✓{done} done")
        if blocked:
            t.append(f"  !{blocked} NEEDS INPUT", "bold red")
        if err:
            t.append(f"   collector error: {err}", "bold red")
        t.append(f"   ⟳{M.poll_interval()}s  {time.strftime('%H:%M:%S')}", "dim")
        return t

    def refresh_data(self):
        rows, q, state, err, ticks = self.collector.snap()
        now = time.time()
        self.query_one("#header", Static).update(self.header_text(rows, q, err))
        table = self.query_one(DataTable)
        desired = []
        for r in rows:
            in_ph, total = issue_times(state, r["issue"], now)
            desired.append((str(r["issue"]),
                            [row_glyph(r), f"#{r['issue']}", r["phase"], in_ph, total,
                             diff_text(r), pr_str(r.get("pr")), checks_text(r.get("pr"))]))
        for i, n in enumerate(q):
            desired.append((f"q{n}",
                            [Text("…", "dim"), Text(f"#{n}", "dim"),
                             Text("queued ◀ next" if i == 0 else "queued", "dim"),
                             "", "", "", "", ""]))
        current = [row.key.value for row in table.ordered_rows]
        if current == [k for k, _ in desired]:
            # same row set → update cells in place, so scroll and cursor are
            # untouched (a clear+re-add yanked the viewport to the top each tick)
            for key, cells in desired:
                for col, val in zip(self._cols, cells):
                    table.update_cell(key, col, val, update_width=True)
        else:
            keep_cursor, keep_y = table.cursor_row, table.scroll_offset.y
            table.clear()
            for key, cells in desired:
                table.add_row(*cells, key=key)
            if desired:
                table.move_cursor(row=min(max(keep_cursor, 0), table.row_count - 1))
                self.call_after_refresh(
                    lambda y=keep_y: table.scroll_to(y=y, animate=False))
        self._rows = rows
        self.render_detail(rows, state)
        evs = (state.get("_events") or [])[-4:]
        act = Text()
        for e in evs:
            ts = time.strftime("%H:%M", time.localtime(e.get("ts", 0)))
            act.append(f"{ts}  #{e.get('issue','?'):<5} {e.get('text','')}\n")
        self.query_one("#activity", Static).update(
            act if evs else Text("no activity yet" if ticks else "waiting for first poll…", "dim"))

    def render_detail(self, rows, state):
        i = self.sel_index(rows)
        head = self.query_one("#dhead", Static)
        rd = self.query_one("#rounds", Static)
        tail = self.query_one("#tail", RichLog)
        if not rows or i >= len(rows):
            head.update(Text("no issue selected", "dim"))
            rd.update("")
            tail.clear()
            return
        r = rows[i]
        now = time.time()
        in_ph, total = issue_times(state, r["issue"], now)
        h = Text()
        h.append(f"#{r['issue']}  ", "bold")
        h.append(f"{r['phase']}   in-phase {in_ph} · total {total}\n")
        pr = r.get("pr")
        h.append(f"PR {pr_str(pr)}   ", "cyan")
        h.append(checks_text(pr))
        deps = r.get("blocked_by") or []
        if deps:
            h.append("\n⛓ blocked by " + " ".join(f"#{d}" for d in deps), "yellow")
        if r["input"]:
            h.append("\n⚠ NEEDS INPUT — press Enter to focus the worker pane", "bold red")
        head.update(h)
        rt = Text()
        for rnd in (r.get("rounds") or [])[-5:]:
            rt.append(f"round {rnd['tag']:<8}", "dim")
            for slot, v in sorted(rnd["slots"].items()):
                lbl, style = VERDICT_STYLE.get(v, (v, "dim"))
                rt.append(f"{slot} ")
                rt.append(lbl, style)
                rt.append("   ")
            rt.append("\n")
        rd.update(rt)
        shown = getattr(self, "_tail_key", None)
        cur = (r["issue"], tuple(r.get("tail") or ()))
        if shown != cur:  # only rewrite when content changed (keeps scroll calm)
            self._tail_key = cur
            tail.clear()
            for ln in r.get("tail") or []:
                tail.write(ln)

    def on_data_table_row_highlighted(self, _):
        rows, _q, state, _e, _t = self.collector.snap()
        self.render_detail(rows, state)

    # -- actions
    def action_cursor(self, d: int):
        t = self.query_one(DataTable)
        if t.row_count:
            t.move_cursor(row=min(max(t.cursor_row + d, 0), t.row_count - 1))

    def _selected_row(self):
        rows = getattr(self, "_rows", [])
        i = self.sel_index(rows)
        return rows[i] if rows and i < len(rows) else None

    def action_focus_worker(self):
        r = self._selected_row()
        if not r or not r.get("pane_id"):
            return
        # Three steps, outermost first. `herdr agent focus <pane>` moves focus WITHIN the
        # pane's workspace; on its own it leaves the viewer wherever they were, so Enter
        # looked like it did nothing from another workspace. Focusing the workspace and
        # then the tab is what actually navigates the view; each call is a no-op when that
        # level is already current, and a missing id is simply skipped.
        steps = [["herdr", "workspace", "focus", r["workspace_id"]] if r.get("workspace_id") else None,
                 ["herdr", "tab", "focus", r["tab_id"]] if r.get("tab_id") else None,
                 ["herdr", "agent", "focus", r["pane_id"]]]
        def go():
            for cmd in steps:
                if cmd:
                    subprocess.run(cmd, capture_output=True)
        self.run_worker(go, thread=True)  # off the UI thread — herdr calls must never block keys

    def action_open_pr(self):
        r = self._selected_row()
        url = (r.get("pr") or {}).get("url") if r else None
        if url:
            self.run_worker(
                lambda: subprocess.run(["open", url], capture_output=True), thread=True)

    def action_poll(self):
        self.collector.poll_now()

    def action_cycle_poll(self):
        nxt = self.collector.cycle_interval()
        self.notify(f"polling every {nxt}s", timeout=2)
        self.refresh_data()  # header shows the new ⟳ cadence immediately

    def action_graph(self):
        self.push_screen(GraphScreen())


def run_dashboard(issues=(), queued=()):
    app = DualAuthorApp(issues, queued)
    try:
        app.run()
    finally:
        app.collector.stop()


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--queued"]
    queued = []
    if "--queued" in sys.argv[1:]:
        qi = sys.argv.index("--queued")
        queued = [x for x in sys.argv[qi + 1].split(",") if x]
        args = [a for a in sys.argv[1:] if a not in ("--queued", sys.argv[qi + 1])]
    run_dashboard(args, queued)
