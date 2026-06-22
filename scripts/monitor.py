#!/usr/bin/env python3
"""dual-author monitor — pure-shell monitoring so the dispatcher LLM doesn't poll.

Everything is NAMESPACED by repo (a slug of `gh repo view`, or DUAL_AUTHOR_NS):
state, queue, brief/review files, and the worker registry live under
/tmp/dual-author/<ns>/. So two mutually-exclusive repos can run concurrent pipelines
(even in one herdr session) without colliding on overlapping issue numbers. Every
context auto-resolves the same ns from its shared git repo — nothing is threaded by
hand. `monitor.py ns` prints the resolved namespace (SKILL.md uses it for paths).

ROUTING vs DISPLAY: a worker's agent name is a DISPLAY string carrying icon + issue +
phase (so the agents page is glanceable), which changes over time and can't be a
routing handle. The dispatcher therefore `register`s each issue's worker against
STABLE handles (terminal id + workspace id); the monitor resolves the live worker pane
from those. Reviewers stay routed by their per-round spawn names (never renamed).

Usage:
  monitor.py ns                                              print the resolved namespace and exit
  monitor.py register <N> --workspace <ws> --pane <pane>     record a worker's stable handles (at dispatch)
  monitor.py unregister <N>                                  drop a worker (on recycle/cleanup)
  monitor.py worker-pane <N>                                 print the worker's current pane id (for agent read/focus)
  monitor.py close-reviewers <N>                             close all non-worker panes in the issue's workspace
  monitor.py watch [<issue>...]                              loop forever, redraw dashboard (run in a pane)
  monitor.py wait  [--seen ev1,ev2] [--queued N,N] <issue>... block until an unseen event, print it, exit 0
  monitor.py review <issue> <tag> --prompt-file F [--cwd D] [--timeout-mins M]
        run ONE full dual-review round as a state machine: spawn codex+claude
        reviewer panes off the worker pane ($HERDR_PANE_ID, since this runs in the
        worker pane), verify registration, name them <ns>-issue-<N>-{codex,claude}-
        <tag>, poll both to idle CONCURRENTLY, verify the review files end with a
        VERDICT line (one re-prompt if not), then ALWAYS close both panes (finally).
        The first VERDICT: FAIL short-circuits the round: the other reviewer is
        cancelled (verdict CANCELLED) so the failing feedback reaches the worker
        immediately. Prints JSON {codex:{file,verdict}, claude:{file,verdict}} and
        exits 0 (verdicts PASS/FAIL/CANCELLED). Reviews land in
        /tmp/dual-author/<ns>/issue-<issue>/<tag>-{codex,claude}.md.
        Prompts pass as a single argv element — immune to typing truncation.

watch with NO issues is the normal mode: every tick it reads the registry for active
workers and the queue from /tmp/dual-author/<ns>/queue.txt (one issue number per line,
dispatch order — maintained by the dispatcher). New issues appear automatically;
unregistered ones drop off; queued ones show as ⏳ rows with the next one marked. It
also renames each worker's agent AND its workspace to one icon-led title
(`⚙️ <ns>-issue-<N> · <phase>`) so the agents page and spaces page both show status.
No restarts needed. Explicit positional issues pin the active set instead (legacy).

watch also auto-sweeps reviewer panes (any non-worker pane in a registered issue's
workspace): idle ≥ 3 min → closed (review file is on disk; nobody reads the pane);
unknown ≥ 10 min → reaped. Workers must treat a vanished reviewer whose review file
ends with a VERDICT line as a completed round, not a failure.

--queued (wait mode, optional) lists issues to render as ⏳; wait still requires an
explicit active list so it can detect missing agents.
Per-issue timing (total elapsed + time in current phase) persists in
/tmp/dual-author/<ns>/monitor-state.json so dashboard restarts don't reset clocks.

Events printed by `wait` (one per line, after a final dashboard render):
  EVENT verdict <N>       worker printed its === ISSUE #N VERDICT === block
  EVENT needs-input <N>   worker is blocked / printed === NEEDS INPUT ===
  EVENT missing <N>       agent issue-<N> disappeared (crashed or closed)
  EVENT all-done          every issue has a verdict

--seen takes handled event ids: verdict-<N>, input-<N>, missing-<N>.
"""
import contextlib
import json
import os
import re
import shlex
import subprocess
import sys
import time

try:
    import fcntl  # POSIX file locking (macOS/Linux) — codex auth-spawn serialization
except ImportError:  # pragma: no cover - non-POSIX; gate degrades to a no-op
    fcntl = None

BASE_ROOT = "/tmp/dual-author"
# Everything below is NAMESPACED by repo (see ns()) so two mutually-exclusive
# repos can run concurrent dual-author pipelines — even in one herdr session —
# without colliding on state files, the queue, brief/review files, or agent
# names (issue numbers overlap across repos). The namespace is auto-derived from
# `gh repo view` in every context (dispatcher cwd, worker worktrees, reviewer
# panes all share one repo → one namespace), so nothing has to be threaded by
# hand. DUAL_AUTHOR_NS overrides it (e.g. two namespaces for one repo).
# Reviewers are one-shot: sustained idle == finished (review file already on disk).
# Grace must outlive the worker's wait→read-file cycle (seconds), not the review itself.
REVIEWER_IDLE_SWEEP_SECS = 180
REVIEWER_UNKNOWN_SWEEP_SECS = 600  # crashed/undetected: keep a forensics window, then reap

# capture ACROSS newlines: the TUI hard-wraps lines mid-word, so grab a window
# after "phase:", strip whitespace, then match against the known phase vocabulary.
# Workers SHOULD emit a trailing " ::" sentinel (SKILL.md) — that variant parses
# exactly even when the TUI wraps adjacent text into the token (the legacy parse
# produced glue like "review-round-12026" = round 1 + a wrapped timestamp).
PHASE_SENT_RE = re.compile(r"\[dual-author\]\s*phase:\s*([\s\S]{0,64}?)::")
PHASE_RE = re.compile(r"\[dual-author\]\s*phase:\s*([\s\S]{0,48})")
PHASE_TOKEN_RE = re.compile(
    r"^(implementing|pushing-pr|review-round-\d+|fixing-round-\d+|awaiting-bots|done|blocked:[A-Za-z0-9-]{0,30})"
)
ICON = {"working": "⚙️", "blocked": "🔴", "idle": "✅", "unknown": "❔", "missing": "💀"}


def sh(*args):
    return subprocess.run(args, capture_output=True, text=True).stdout


def agents():
    try:
        d = json.loads(sh("herdr", "agent", "list"))
        return {a.get("name") or "": a for a in d["result"]["agents"]}
    except Exception:
        return {}


# ---- PR-merge ground truth -------------------------------------------------
# Pane text is a LOSSY completion signal: the verdict block scrolls out of the
# read window, sessions pause at usage limits, and TUI re-renders eat lines.
# Three completions were silently missed in one run before this existed. The
# authoritative signal is the PR itself: issue/<N> branch merged ⇒ finished.
_REPO = None


def _repo():
    global _REPO
    if _REPO is None:
        _REPO = sh("gh", "repo", "view", "--json", "nameWithOwner",
                   "-q", ".nameWithOwner").strip()
    return _REPO


# ---- namespacing -----------------------------------------------------------
_NS = None


def _slug(s):
    s = re.sub(r"[^A-Za-z0-9]+", "-", (s or "").strip().lower()).strip("-")
    return s or "default"


def ns():
    """Namespace for this run — DUAL_AUTHOR_NS, else a slug of owner/repo.

    The owner/repo fallback comes from `gh repo view`, which resolves against the
    CURRENT PANE'S CWD. Workers run inside the worktree so they resolve correctly,
    but the dispatcher's dashboard pane may sit outside the repo (in ~ or wherever
    herdr's origin pane landed), where `gh repo view` finds nothing and this falls
    back to "default" — a DIFFERENT namespace from the workers', so the dashboard
    reads an empty registry and renames nothing. Any process launched in a pane
    that isn't guaranteed to be in the repo (the dashboard above all) MUST be given
    DUAL_AUTHOR_NS explicitly; do not rely on cwd agreeing across panes."""
    global _NS
    if _NS is None:
        _NS = os.environ.get("DUAL_AUTHOR_NS") or _slug(_repo())
    return _NS


def base():
    """Per-namespace state dir: /tmp/dual-author/<ns>/..."""
    return os.path.join(BASE_ROOT, ns())


def state_path():
    return os.path.join(base(), "monitor-state.json")


def queue_path():
    return os.path.join(base(), "queue.txt")


def worker_display(issue, icon, phase):
    """Worker agent's DISPLAY name — what the agents page shows. Icon-led so status
    is glanceable, then issue id, then phase. This string changes as status/phase
    change, so it must NOT be used for routing — routing goes through the registry
    (stable terminal id). Distinct per issue, so two live workers never collide."""
    return f"{icon} {ns()}-issue-{issue} · {phase}"


# ---- worker registry ------------------------------------------------------
# The worker's display name now carries icon+phase, so it can't double as the
# routing handle. Instead the dispatcher registers each issue's worker against
# STABLE handles (terminal id + workspace id) at dispatch; the monitor resolves
# the worker's live pane from those every tick. terminal_id survives pane
# renumbering (panes compact when reviewer splits close); workspace_id is the
# fallback. Reviewers stay routed by their spawn names (short-lived, never
# renamed by watch), so only the worker needs this.

def registry_path():
    return os.path.join(base(), "registry.json")


def load_registry():
    try:
        with open(registry_path()) as f:
            return json.load(f)
    except Exception:
        return {}


def save_registry(reg):
    p = registry_path()
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = f"{p}.{os.getpid()}.tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(reg, f)
        os.replace(tmp, p)
    except OSError:
        pass


def _terminal_of(pane):
    try:
        return json.loads(sh("herdr", "pane", "get", pane))["result"]["pane"].get("terminal_id")
    except Exception:
        return None


def register(issue, workspace, pane):
    reg = load_registry()
    reg[str(issue)] = {"ws": workspace, "term": _terminal_of(pane), "root_pane": pane}
    save_registry(reg)


def unregister(issue):
    reg = load_registry()
    reg.pop(str(issue), None)
    save_registry(reg)


def _worker_agent(ag, issue, reg=None):
    """The worker agent dict for an issue, resolved by stable handle. Prefers the
    registered terminal id (survives pane renumbering); falls back to the recorded
    root pane id."""
    reg = reg if reg is not None else load_registry()
    e = reg.get(str(issue))
    if not e:
        return None
    term, root = e.get("term"), e.get("root_pane")
    if term:
        for a in ag.values():
            if a.get("terminal_id") == term:
                return a
    for a in ag.values():
        if a.get("pane_id") == root:
            return a
    return None


def worker_pane(issue):
    a = _worker_agent(agents(), issue)
    return a.get("pane_id") if a else None


_PR_CACHE = {}  # issue -> (last_check_ts, merged_bool); merged is terminal
PR_POLL_SECS = 60  # per-issue gh poll budget — cheap enough, far under rate limits


def pr_merged(issue):
    now = time.time()
    ts, merged = _PR_CACHE.get(issue, (0.0, False))
    if merged:
        return True
    if now - ts < PR_POLL_SECS:
        return False
    repo = _repo()
    if repo:
        out = sh("gh", "pr", "list", "--repo", repo, "--head", f"issue/{issue}",
                 "--state", "merged", "--json", "number")
        try:
            merged = bool(json.loads(out))
        except Exception:
            merged = False
    _PR_CACHE[issue] = (now, merged)
    return merged


def discover_issues(ag):
    """Active issues = the registry keys for THIS namespace (the dispatcher
    registers each worker at dispatch and unregisters on recycle). Registry lives
    under /tmp/dual-author/<ns>/, so a concurrent run in another repo is invisible.
    `ag` is unused now but kept for call-site compatibility."""
    return sorted(load_registry().keys(), key=int)


def read_queue(active):
    try:
        with open(queue_path()) as f:
            q = [ln.strip().lstrip("#") for ln in f if ln.strip()]
    except OSError:
        return []
    return [n for n in q if n not in set(active)]


def snapshot(issues):
    ag = agents()
    reg = load_registry()
    rows = []
    for n in issues:
        a = _worker_agent(ag, n, reg)
        if not a:
            # a vanished agent whose PR merged FINISHED — report verdict, not missing
            rows.append({"issue": n, "status": "missing", "phase": "-",
                         "verdict": pr_merged(n), "input": False,
                         "workspace_id": (reg.get(str(n)) or {}).get("ws"), "pane_id": None})
            continue
        text = sh("herdr", "pane", "read", a["pane_id"], "--source", "recent-unwrapped", "--lines", "120")
        # phases are single hyphenated tokens; the agent TUI hard-wraps mid-word.
        # Prefer the " ::"-sentinel form (exact through wrapping); fall back to
        # stripping ALL whitespace from the window and keeping the leading token.
        phases = []
        for p in PHASE_SENT_RE.findall(text):
            tok = PHASE_TOKEN_RE.match(re.sub(r"\s+", "", p))
            if tok:
                phases.append(tok.group(0))
        if not phases:
            for p in PHASE_RE.findall(text):
                tok = PHASE_TOKEN_RE.match(re.sub(r"\s+", "", p))
                if tok:
                    phases.append(tok.group(0))
        phase = phases[-1] if phases else "starting"
        # completion = ANY of: verdict block in window, done phase marker, or the
        # PR-merge ground truth (pane text alone is lossy — scroll/limits/redraws)
        verdict = (f"=== ISSUE #{n} VERDICT ===" in text) or phase == "done" or pr_merged(n)
        rows.append({
            "issue": n,
            "status": a.get("agent_status", "unknown"),
            "phase": phase,
            "verdict": verdict,
            "input": "=== NEEDS INPUT" in text,
            "workspace_id": a.get("workspace_id"),
            "pane_id": a.get("pane_id"),
        })
    return rows


def _round_sentinel(issue):
    return os.path.join(base(), f"round-active-{issue}")


def active_round_issues():
    """Issue numbers with a live `monitor.py review <N> ...` process — their reviewer
    panes are owned by that runner and must not be swept.

    Each runner drops a pid sentinel in THIS namespace's dir (review_round), so
    detection is repo-scoped — a concurrent run's round for the same issue number
    in another repo can't make us over-protect. Stale sentinels (dead pid) are
    pruned on read."""
    issues = set()
    try:
        names = os.listdir(base())
    except OSError:
        return issues
    for fn in names:
        m = re.match(r"round-active-(\d+)$", fn)
        if not m:
            continue
        path = os.path.join(base(), fn)
        try:
            pid = int(open(path).read().strip())
            os.kill(pid, 0)  # alive?
            issues.add(m.group(1))
        except (OSError, ValueError):
            try:
                os.remove(path)  # stale/garbage sentinel
            except OSError:
                pass
    return issues


def sweep_reviewers(state, ag):
    """Deterministically close finished reviewer panes (watch mode, every tick).

    A reviewer is identified STRUCTURALLY: any agent sharing a registered issue's
    workspace that is NOT that issue's worker (resolved via the registry). One that
    has been idle for REVIEWER_IDLE_SWEEP_SECS is done — its review file is on disk
    and the worker reads the FILE, never the pane. unknown-status reviewers (crashed)
    get a longer forensics window, then are reaped too. Workers still sweep their own
    panes per SKILL.md; this is the backstop that makes cleanup unconditional.
    """
    now = time.time()
    rv = state.setdefault("_reviewers", {})
    # NEVER sweep a reviewer whose round runner is still alive — the runner owns its
    # panes and closes them in finally. Sweeping under it makes the runner wait on a
    # dead pane to its timeout (the two cleanup systems racing). active_round_issues()
    # reads the live round sentinels for this namespace.
    protected = active_round_issues()
    reg = load_registry()
    ws_issue = {e.get("ws"): n for n, e in reg.items()}            # workspace -> issue
    worker_panes = {a.get("pane_id") for n in reg
                    for a in [_worker_agent(ag, n, reg)] if a}     # the workers themselves
    candidates = {}
    for name, a in ag.items():
        issue = ws_issue.get(a.get("workspace_id"))
        if not issue or issue in protected:
            continue
        if a.get("pane_id") in worker_panes:
            continue  # the worker pane, never a reviewer
        candidates[a["pane_id"]] = a
    for key, a in candidates.items():
        status = a.get("agent_status", "unknown")
        rec = rv.setdefault(key, {})
        if rec.get("status") != status:
            rec["status"] = status
            rec["since"] = now
        grace = {"idle": REVIEWER_IDLE_SWEEP_SECS, "unknown": REVIEWER_UNKNOWN_SWEEP_SECS}.get(status)
        if grace is not None and now - rec.get("since", now) >= grace:
            sh("herdr", "pane", "close", a["pane_id"])
            rv.pop(key, None)
    for key in list(rv):  # prune records for agents/panes that already vanished
        if key not in candidates:
            rv.pop(key, None)


# ---------------- review-round state machine ----------------
# States per reviewer: SPAWN -> VERIFY (retry once) -> RUN (working->idle) ->
# COLLECT (re-prompt once if file lacks VERDICT) -> CLEAN (always).
# RUN polls both reviewers concurrently: the first VERDICT: FAIL short-circuits the
# round — the other reviewer is cancelled (CANCELLED) and the failing review goes
# straight back to the worker (the author) to fix against.

def _run(*args, timeout=None):
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def _agent_pane(name):
    try:
        d = json.loads(sh("herdr", "agent", "get", name))
        return d["result"]["agent"]["pane_id"]
    except Exception:
        return None


def _agent_alive(name):
    """Registered AND actually running (working/idle) — a renamed bare shell is
    'unknown' and does not count."""
    try:
        d = json.loads(sh("herdr", "agent", "get", name))["result"]["agent"]
        return d["pane_id"] if d.get("agent_status") in ("working", "idle") else None
    except Exception:
        return None


def _tool_argv(tool):
    """The reviewer launch argv. claude is pinned to Opus at high effort
    (deterministic regardless of the user's session defaults).

    codex launches non-interactively-approved: `--ask-for-approval never` so it
    never stops to ask the user to run a command (its default policy prompts
    per-command, which stalls an unattended reviewer — claude doesn't because the
    worktree is pre-trusted). The sandbox stays ON at `workspace-write` (no network,
    no writes outside the workspace) — we only widen it to also allow the
    dual-author temp dir, since the reviewer must write its VERDICT file there
    (outside the worktree). realpath resolves the macOS /tmp -> /private/tmp symlink
    so the sandbox policy matches the path codex actually writes to."""
    if tool == "claude":
        return ["claude", "--model", "opus", "--effort", "high"]
    review_root = os.path.realpath(os.path.dirname(base()))  # /tmp/dual-author, resolved
    return ["codex", "--ask-for-approval", "never", "--sandbox", "workspace-write",
            "-c", f'sandbox_workspace_write.writable_roots=["{review_root}"]']


# Machine-global lock that serializes codex reviewer STARTUP (the auth/token-refresh
# window) across ALL dual-author runs on this host. codex on ChatGPT-subscription auth
# authenticates against one shared ~/.codex/auth.json whose refresh token is single-use
# (rotating): if several reviewers start at once they race the refresh and the losers get
# 401 "refresh token has already been used" and exit during startup -> the slot reports
# SPAWN-FAILED (claude reviewers are immune — different credential, no shared rotation).
# Serializing ONLY the startup window lets each refresh complete + write back before the
# next codex starts (after the first refresh the token is valid for hours, so the rest
# read it and start fast); review EXECUTION stays fully parallel. The path is fixed/global
# (NOT namespaced) because the contended file is shared across every repo/namespace.
_CODEX_AUTH_LOCK = "/tmp/dual-author-codex-auth.lock"


@contextlib.contextmanager
def _codex_auth_gate(tool):
    """Hold an exclusive lock around a codex reviewer's startup; no-op otherwise."""
    if tool != "codex" or fcntl is None:
        yield
        return
    f = open(_CODEX_AUTH_LOCK, "w")
    try:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(f, fcntl.LOCK_UN)
        finally:
            f.close()


def _spawn_reviewer(name, base_pane, split, cwd, tool, prompt):
    """Spawn one reviewer; return its pane_id or None. Verifies a LIVE agent.

    Primary: `herdr agent start` (prompt as argv — no typing). Verified via the
    agent registry, never by parsing stdout shape. Fallback: split a pane and run
    a LAUNCH SCRIPT — the pane types only a short script path, so a socket hiccup
    cannot truncate the prompt (typing the long command directly is how rounds
    used to end up as dead half-typed shells).

    codex spawns are serialized through ``_codex_auth_gate`` so concurrent reviewers
    don't race the single-use refresh token in the shared ~/.codex/auth.json (the
    SPAWN-FAILED cause); the gate is held only until the agent is alive (auth done).
    """
    with _codex_auth_gate(tool):
        return _spawn_reviewer_inner(name, base_pane, split, cwd, tool, prompt)


def _spawn_reviewer_inner(name, base_pane, split, cwd, tool, prompt):
    _run("herdr", "agent", "start", name, "--tab", base_pane, "--split", split,
         "--no-focus", "--cwd", cwd, "--", *_tool_argv(tool), prompt)
    for _ in range(6):  # agent start registers the name itself if it worked
        p = _agent_alive(name)
        if p:
            return p
        time.sleep(2)
    # fallback: script-file launch in a fresh pane
    try:
        pane = json.loads(sh("herdr", "pane", "split", base_pane, "--direction", split, "--no-focus"))["result"]["pane"]["pane_id"]
    except Exception:
        return None
    os.makedirs(base(), exist_ok=True)
    script = os.path.join(base(), f"launch-{name}.sh")
    with open(script, "w") as f:
        f.write(f"#!/bin/zsh\ncd {shlex.quote(cwd)}\nexec {' '.join(_tool_argv(tool))} {shlex.quote(prompt)}\n")
    os.chmod(script, 0o755)
    _run("herdr", "pane", "run", pane, script)
    for _ in range(8):
        if _agent_alive(name):
            return pane
        _run("herdr", "agent", "rename", pane, name)
        time.sleep(3)
    if _agent_alive(name):
        return pane
    sh("herdr", "pane", "close", pane)  # dead launch: reap, signal failure
    return None


def _wait_status(name, status, timeout_ms):
    return _run("herdr", "agent", "wait", name, "--status", status, "--timeout", str(timeout_ms)).returncode == 0


def _verdict_of(path):
    try:
        with open(path) as f:
            for line in reversed(f.read().strip().splitlines()):
                if line.strip().startswith("VERDICT:"):
                    return line.strip().split(":", 1)[1].strip()
    except OSError:
        pass
    return None


def review_round(issue, tag, prompt, cwd, timeout_s):
    rd = os.path.join(base(), f"issue-{issue}")
    os.makedirs(rd, exist_ok=True)
    # This runs IN the worker pane, so $HERDR_PANE_ID is the worker pane — the
    # natural, focus-proof anchor to split reviewers off. Fall back to the registry
    # (worker resolved by stable terminal id) if the env var is somehow absent.
    base_pane = os.environ.get("HERDR_PANE_ID") or worker_pane(issue)
    if not base_pane:
        print(json.dumps({"error": f"cannot resolve worker pane for issue {issue} to anchor splits"}))
        return 1
    # reviewer agent names are routing handles for THIS round only (watch never
    # renames reviewers), so keep them stable/structured — not display strings.
    worker = f"{ns()}-issue-{issue}"
    sentinel = _round_sentinel(issue)  # tells watch's sweep we own these panes
    with open(sentinel, "w") as f:
        f.write(str(os.getpid()))
    plan = {}
    # <base>/codex-down sentinel (dispatcher-managed): codex CLI is
    # quota-dead at OpenAI. Fill the codex SLOT with a second independent claude
    # instance so every round still gets two reviewers. The result key stays
    # "codex" (callers read it structurally); "tool" records the substitution.
    codex_down = os.path.exists(os.path.join(base(), "codex-down"))
    for slot, split in (("codex", "right"), ("claude", "down")):
        tool, name, outfile = slot, f"{worker}-{slot}-{tag}", f"{rd}/{tag}-{slot}.md"
        if slot == "codex" and codex_down:
            tool = "claude"
            name = f"{worker}-claude-{tag}x"   # still matches the sweep regex
            outfile = f"{rd}/{tag}-claude2.md"
        full = (f"{prompt} Write your FULL review to {outfile}, ending the file "
                f"with VERDICT: PASS or VERDICT: FAIL on its own line.")
        plan[slot] = {"name": name, "file": outfile, "tool": tool,
                      "prompt": full, "split": split, "pane": None}
    results = {}
    try:
        for slot, p in plan.items():  # SPAWN + VERIFY (one retry)
            p["pane"] = (_spawn_reviewer(p["name"], base_pane, p["split"], cwd, p["tool"], p["prompt"])
                         or _spawn_reviewer(p["name"], base_pane, p["split"], cwd, p["tool"], p["prompt"]))
        # COLLECT helper: read a finished reviewer's verdict, re-prompt once if absent.
        def _collect(p):
            v = _verdict_of(p["file"])
            if v is None:
                pane = _agent_pane(p["name"])
                if pane:
                    _run("herdr", "pane", "send-text", pane,
                         f"Your review file {p['file']} is missing or lacks a final VERDICT line. Write it now, ending with VERDICT: PASS or VERDICT: FAIL.")
                    _run("herdr", "pane", "send-keys", pane, "Enter")
                    _wait_status(p["name"], "working", 30_000)
                    _wait_status(p["name"], "idle", (timeout_s // 2) * 1000)
                    v = _verdict_of(p["file"])
            return v or "MISSING"

        # RUN — poll both reviewers CONCURRENTLY rather than waiting one out fully.
        # The reviewers run in parallel; the moment ONE returns VERDICT: FAIL we
        # cancel the other (close its pane) and return — the author has to fix
        # against the failing review regardless, so a second opinion buys nothing
        # and only costs wall-clock. The cancelled slot is reported as CANCELLED so
        # the worker knows it was short-circuited, not broken. (PASS reviewers still
        # both run to completion — we only short-circuit on the first FAIL.)
        pending = []
        for slot, p in plan.items():
            if p["pane"]:
                _wait_status(p["name"], "working", 60_000)  # guard startup idle; ok to miss
                pending.append(slot)
            else:
                results[slot] = {"file": p["file"], "verdict": "SPAWN-FAILED", "tool": p["tool"]}
        deadline = time.time() + timeout_s
        failed = False
        while pending and not failed and time.time() < deadline:
            for slot in list(pending):  # iterate a copy; we mutate pending below
                p = plan[slot]
                # short idle-poll so the OTHER reviewer's FAIL can interrupt promptly
                if not _wait_status(p["name"], "idle", 5_000):
                    continue  # still working (or transiently unknown) — re-poll
                v = _collect(p)
                results[slot] = {"file": p["file"], "verdict": v, "tool": p["tool"]}
                pending.remove(slot)
                if v == "FAIL":
                    failed = True
                    break
        # Resolve whoever is still pending: cancel them on a FAIL short-circuit,
        # otherwise (deadline hit) collect whatever they managed to write.
        for slot in pending:
            p = plan[slot]
            if failed:
                pane = _agent_pane(p["name"]) or p["pane"]
                if pane:
                    _run("herdr", "pane", "close", pane)
                results[slot] = {"file": p["file"], "verdict": "CANCELLED", "tool": p["tool"]}
            else:
                results[slot] = {"file": p["file"], "verdict": _collect(p), "tool": p["tool"]}
    finally:  # CLEAN — unconditional, name-independent (uses tracked pane ids)
        for p in plan.values():
            pane = _agent_pane(p["name"]) or p["pane"]
            if pane:
                _run("herdr", "pane", "close", pane)
        try:
            os.remove(sentinel)
        except OSError:
            pass
    print(json.dumps(results))
    # CANCELLED is a clean outcome (the round was decided by the other reviewer's
    # FAIL), so it counts toward exit 0 alongside PASS/FAIL; only MISSING/
    # SPAWN-FAILED leave a slot genuinely undecided.
    return 0 if all(r["verdict"] in ("PASS", "FAIL", "CANCELLED") for r in results.values()) else 1


def load_state():
    try:
        with open(state_path()) as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state):
    # pid-unique tmp: watch + wait (+ review) run concurrently; a shared tmp name
    # races on os.replace (FileNotFoundError when the other process wins).
    sp = state_path()
    os.makedirs(os.path.dirname(sp), exist_ok=True)
    tmp = f"{sp}.{os.getpid()}.tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(state, f)
        os.replace(tmp, sp)
    except OSError:
        pass  # monitoring state is advisory — never crash the loop over it


def update_timing(state, rows):
    now = time.time()
    for r in rows:
        st = state.setdefault(str(r["issue"]), {})
        if "done" in st and not r["verdict"]:  # fresh run of a previously finished issue
            st.clear()
        st.setdefault("start", now)
        if r["phase"] != st.get("phase"):
            st["phase"] = r["phase"]
            st["phase_start"] = now
        if r["verdict"] and "done" not in st:
            st["done"] = now
    save_state(state)
    return state


def fmt_dur(secs):
    secs = int(max(0, secs))
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def row_icon(r):
    """Single status glyph for a row — shared by the dashboard and the spaces-page
    (workspace) label so both read identically: verdict ✅, needs-input/blocked 🔴,
    else the live agent_status (⚙️ working / ✅ idle / ❔ unknown / 💀 missing)."""
    if r["verdict"]:
        return "✅"
    if r["input"] or r["status"] == "blocked":
        return "🔴"
    return ICON.get(r["status"], "❔")


def render(rows, state, queued=()):
    now = time.time()
    out = [
        f"dual-author — {time.strftime('%H:%M:%S')}",
        f"{'issue':<9}{'state':<6}{'phase':<22}{'in-phase':<10}total",
    ]
    for r in rows:
        icon = row_icon(r)
        st = state.get(str(r["issue"]), {})
        end = st.get("done", now)
        total = fmt_dur(end - st["start"]) if "start" in st else "-"
        in_phase = "-" if "done" in st or "phase_start" not in st else fmt_dur(now - st["phase_start"])
        out.append(f"#{str(r['issue']):<8}{icon:<5}{r['phase']:<22}{in_phase:<10}{total}")
    for i, n in enumerate(queued):
        label = "queued ◀ next" if i == 0 else "queued"
        out.append(f"#{str(n):<8}{'⏳':<5}{label:<22}{'-':<10}-")
    return "\n".join(out)


def events(rows, seen):
    ev = []
    for r in rows:
        n = r["issue"]
        if r["verdict"] and f"verdict-{n}" not in seen:
            ev.append(f"EVENT verdict {n}")
        if (r["input"] or r["status"] == "blocked") and not r["verdict"] and f"input-{n}" not in seen:
            ev.append(f"EVENT needs-input {n}")
        if r["status"] == "missing" and f"missing-{n}" not in seen:
            ev.append(f"EVENT missing {n}")
    if rows and all(r["verdict"] for r in rows):
        ev.append("EVENT all-done")
    return ev


def main():
    modes = ("watch", "wait", "review", "ns", "register", "unregister",
             "worker-pane", "close-reviewers")
    if len(sys.argv) < 2 or sys.argv[1] not in modes:
        print(__doc__)
        sys.exit(2)
    mode, rest = sys.argv[1], sys.argv[2:]

    if mode == "ns":
        # Resolved namespace for this repo — SKILL.md uses it to build the
        # matching /tmp/dual-author/<ns> paths.
        print(ns())
        return

    if mode == "register":
        # register <N> --workspace <ws> --pane <root_pane>: record the worker's
        # stable handles so the dispatcher can resolve it after its display name
        # starts carrying icon+phase. Called once per issue at dispatch.
        issue, args = rest[0].lstrip("#"), rest[1:]
        opts = {"workspace": None, "pane": None}
        while args:
            opts[args[0].lstrip("-")] = args[1]
            args = args[2:]
        register(issue, opts["workspace"], opts["pane"])
        return

    if mode == "unregister":
        unregister(rest[0].lstrip("#"))
        return

    if mode == "worker-pane":
        # print the worker's CURRENT pane id (its display name is not addressable);
        # SKILL.md uses this for `herdr agent read/focus`.
        p = worker_pane(rest[0].lstrip("#"))
        if p:
            print(p)
        else:
            sys.exit(1)
        return

    if mode == "close-reviewers":
        # close every non-worker pane in an issue's workspace (verdict-time sweep).
        issue = rest[0].lstrip("#")
        ag = agents()
        reg = load_registry()
        e = reg.get(str(issue)) or {}
        wa = _worker_agent(ag, issue, reg)
        wp = wa.get("pane_id") if wa else None
        for a in ag.values():
            if a.get("workspace_id") == e.get("ws") and a.get("pane_id") != wp:
                sh("herdr", "pane", "close", a["pane_id"])
        return

    if mode == "review":
        issue, tag, args = rest[0].lstrip("#"), rest[1], rest[2:]
        opts = {"cwd": os.getcwd(), "timeout-mins": "15", "prompt-file": None}
        while args:
            k = args[0].lstrip("-")
            opts[k] = args[1]
            args = args[2:]
        if not opts["prompt-file"]:
            print("review requires --prompt-file", file=sys.stderr)
            sys.exit(2)
        with open(opts["prompt-file"]) as f:
            prompt = f.read().strip()
        sys.exit(review_round(issue, tag, prompt, opts["cwd"], int(opts["timeout-mins"]) * 60))

    seen = set()
    queued = []
    while rest and rest[0] in ("--seen", "--queued"):
        if rest[0] == "--seen":
            seen = set(rest[1].split(","))
        else:
            queued = [q for q in rest[1].split(",") if q]
        rest = rest[2:]
    issues = rest

    if mode == "watch":
        labels = {}  # (kind, id) -> last label set, to avoid rename churn
        while True:
            ag = agents()
            active = issues or discover_issues(ag)
            q = queued or read_queue(active)
            rows = snapshot(active)
            state = update_timing(load_state(), rows)
            sweep_reviewers(state, ag)
            save_state(state)
            print("\033[2J\033[H" + render(rows, state, q), flush=True)
            for r in rows:
                # one icon-LED title so status is glanceable even when the sidebar
                # truncates: issue id then current phase. Applied to BOTH the
                # workspace (spaces page) and the worker agent (agents page).
                label = f"{row_icon(r)} {ns()}-issue-{r['issue']} · {r['phase']}"
                ws = r.get("workspace_id")
                if ws and labels.get(("ws", ws)) != label:
                    sh("herdr", "workspace", "rename", ws, label)
                    labels[("ws", ws)] = label
                pane = r.get("pane_id")  # worker still live → its agents-page title
                if pane and labels.get(("agent", pane)) != label:
                    sh("herdr", "agent", "rename", pane, label)
                    labels[("agent", pane)] = label
            time.sleep(5)
    elif mode == "wait":
        while True:
            rows = snapshot(issues)
            state = update_timing(load_state(), rows)
            ev = events(rows, seen)
            if ev:
                print(render(rows, state, queued or read_queue(issues)))
                print("\n".join(ev))
                sys.exit(0)
            time.sleep(5)
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
