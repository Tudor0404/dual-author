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
  monitor.py config [<a.b.c>]                                print resolved config (all as JSON, or one dotted key)
  monitor.py author-launch --pane <P> --prompt-file <F> [--cwd <D>]
                                                             launch the configured AUTHOR agent (claude|codex) in a pane
  monitor.py register <N> --workspace <ws> --pane <pane>     record a worker's stable handles (at dispatch)
  monitor.py unregister <N>                                  drop a worker (on recycle/cleanup)
  monitor.py worker-pane <N>                                 print the worker's current pane id (for agent read/focus)
  monitor.py close-reviewers <N>                             close all non-worker panes in the issue's workspace
  monitor.py watch [--legacy] [<issue>...]                   full-screen Textual dashboard (run in a pane);
                                                             --legacy or a non-TTY = old plain-text render
  monitor.py collect [--queued N,N] [<issue>...]             headless data loop for the dashboard: one
                                                             collect_tick per poll interval (5/20/60s via
                                                             <base>/poll-interval, dashboard [p] key) ->
                                                             <base>/dashboard.json (spawned by
                                                             dashboard.py; exits with it)
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
(`⚙️ <ns>-issue-<N> · <phase>`), and the issue's TAB to `#<N> · <repo-name>`, so the
agents page, spaces page, and tab strip all show status. No restarts needed. Explicit
positional issues pin the active set instead (legacy).

watch in a TTY runs the full-screen Textual dashboard (dashboard.py, via `uv run` —
uv provisions python+textual in a cached env on first use): issues table + detail
panel (PR/checks, review-round verdicts, live worker-output tail), activity feed,
and a [g] pipeline-graph view of every issue — all clipped to one viewport.
Keys: ↑↓/jk select, Enter focus worker pane, g graph, o open PR, r poll, q quit
(quitting only closes the dashboard; the pipeline keeps running). `--legacy`, a
non-TTY stdout, or missing uv falls back to the old plain-text render.

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
import copy
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
# Tunables (reviewer sweep windows, codex-down TTL, PR-poll budget) and all
# model/agent/concurrency choices now live in a TOML config — see cfg() below and
# config.toml in the skill root. Reference them via cfg()["timeouts"][...] etc. so a
# user edit takes effect without touching this file.

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


# ---- configuration ---------------------------------------------------------
# Everything a user is likely to want to change — which agent AUTHORS each issue
# (claude OR codex), the review panel (any mix of codex/claude, models, effort),
# concurrency, merge policy, and the monitor tunables — is read from a TOML file.
# No tomllib on Python 3.9 (macOS system python), so a tiny dependency-free parser
# handles the subset the config uses. Precedence (low → high, later wins):
#   1. config.toml shipped in the skill root (the documented defaults)
#   2. <repo>/.dual-author.toml (per-repo override, committable)
#   3. $DUAL_AUTHOR_CONFIG (explicit path)
# and DEFAULTS below underpins all three so the skill still runs with no file.

DEFAULTS = {
    # The agent that IMPLEMENTS each issue — the "main authoring". Set tool="codex"
    # to have codex author. codex authors need to push/gh/run tools, so they default
    # to a permissive sandbox (unlike the read-only-ish reviewer codex).
    "author": {"tool": "claude", "model": "opus", "effort": "high",
               "extra_args": [], "codex_sandbox": "danger-full-access",
               "codex_approval": "never", "codex_model": "gpt-5.3-codex",
               "codex_effort": "high"},
    "dispatch": {"parallel": 3},
    "review": {
        "timeout_mins": 15,
        # The review panel. Order sets split placement (right, down, …). Each entry:
        # slot (stable id used in file/agent names), tool (codex|claude), and
        # optional model/effort/extra_args (+ codex_sandbox/codex_approval).
        "reviewers": [
            {"slot": "codex", "tool": "codex", "model": "", "effort": "",
             "codex_sandbox": "workspace-write", "codex_approval": "never",
             "extra_args": []},
            {"slot": "claude", "tool": "claude", "model": "sonnet",
             "effort": "high", "extra_args": []},
        ],
    },
    # Merge policy for a clean PR (WORKER step 4). enabled=false leaves PRs ready but
    # unmerged for a human to merge.
    "merge": {"enabled": True, "auto": True, "method": "squash", "delete_branch": True},
    "timeouts": {"reviewer_idle_sweep_secs": 180, "reviewer_unknown_sweep_secs": 600,
                 "codex_down_ttl_secs": 90 * 60, "pr_poll_secs": 60},
}


def _toml_strip_comment(line):
    out, q = [], None
    for c in line:
        if q:
            out.append(c)
            if c == q:
                q = None
        elif c in ('"', "'"):
            q = c
            out.append(c)
        elif c == "#":
            break
        else:
            out.append(c)
    return "".join(out).rstrip()


def _toml_split_array(s):
    parts, cur, q, depth = [], [], None, 0
    for c in s:
        if q:
            cur.append(c)
            if c == q:
                q = None
        elif c in ('"', "'"):
            q = c
            cur.append(c)
        elif c == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            if c == "[":
                depth += 1
            elif c == "]":
                depth -= 1
            cur.append(c)
    if "".join(cur).strip():
        parts.append("".join(cur))
    return parts


def _toml_value(s):
    s = s.strip()
    if not s:
        return ""
    if s[0] == "[" and s[-1] == "]":
        return [_toml_value(x) for x in _toml_split_array(s[1:-1])]
    if len(s) >= 2 and s[0] == s[-1] and s[0] in ('"', "'"):
        inner = s[1:-1]
        if s[0] == '"':
            inner = (inner.replace('\\"', '"').replace("\\n", "\n")
                     .replace("\\t", "\t").replace("\\\\", "\\"))
        return inner
    if s == "true":
        return True
    if s == "false":
        return False
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s)
    except ValueError:
        pass
    return s


def _toml_table(root, parts):
    d = root
    for p in parts:
        nxt = d.get(p)
        if isinstance(nxt, list):
            d = nxt[-1]
        elif isinstance(nxt, dict):
            d = nxt
        else:
            nxt = {}
            d[p] = nxt
            d = nxt
    return d


def _toml_load(text):
    """Parse the TOML subset the config uses: [tables], [[arrays.of.tables]],
    string/int/float/bool scalars, and inline string arrays. Good enough for a
    hand-edited config file whose shape we control; not a general TOML parser."""
    root = {}
    cur = root
    for raw in text.splitlines():
        line = _toml_strip_comment(raw).strip()
        if not line:
            continue
        if line.startswith("[[") and line.endswith("]]"):
            parts = [p.strip() for p in line[2:-2].split(".")]
            parent = _toml_table(root, parts[:-1])
            lst = parent.get(parts[-1])
            if not isinstance(lst, list):
                lst = []
                parent[parts[-1]] = lst
            cur = {}
            lst.append(cur)
        elif line.startswith("[") and line.endswith("]"):
            parts = [p.strip() for p in line[1:-1].split(".")]
            cur = _toml_table(root, parts)
        elif "=" in line:
            k, v = line.split("=", 1)
            cur[k.strip()] = _toml_value(v)
    return root


def _deep_merge(base_d, over):
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(base_d.get(k), dict):
            _deep_merge(base_d[k], v)
        else:
            base_d[k] = v  # scalars + lists (e.g. reviewers) replace wholesale
    return base_d


def _config_paths():
    paths = [os.path.normpath(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "config.toml"))]
    root = sh("git", "rev-parse", "--show-toplevel").strip()
    if root:
        paths.append(os.path.join(root, ".dual-author.toml"))
    env = os.environ.get("DUAL_AUTHOR_CONFIG")
    if env:
        paths.append(env)
    return paths  # low → high precedence


_CFG = None


def cfg():
    global _CFG
    if _CFG is None:
        merged = copy.deepcopy(DEFAULTS)
        for path in _config_paths():
            try:
                with open(path) as f:
                    _deep_merge(merged, _toml_load(f.read()))
            except OSError:
                pass  # file absent — fine, next layer / defaults cover it
            except Exception as e:  # a malformed file must not brick the pipeline
                sys.stderr.write(f"[dual-author] warning: could not parse {path}: {e}\n")
        _CFG = merged
    return _CFG


def cfg_get(dotted):
    d = cfg()
    for p in dotted.split("."):
        if isinstance(d, dict):
            d = d.get(p)
        else:
            return None
    return d


def _build_argv(spec, role):
    """Launch argv for one agent from its config spec. role is 'author' or 'review'
    — it only changes codex sandbox defaults (an author must push/gh/run tools; a
    reviewer stays sandboxed to workspace-write + the review temp dir)."""
    tool = spec.get("tool", "claude")
    extra = list(spec.get("extra_args") or [])
    if tool == "codex":
        approval = spec.get("codex_approval", "never")
        sandbox = spec.get("codex_sandbox") or (
            "danger-full-access" if role == "author" else "workspace-write")
        argv = ["codex", "--ask-for-approval", approval, "--sandbox", sandbox]
        if role != "author" and sandbox == "workspace-write":
            # reviewer must write its VERDICT file under /tmp/dual-author (outside the
            # worktree); realpath resolves macOS /tmp -> /private/tmp for the policy.
            review_root = os.path.realpath(os.path.dirname(base()))
            argv += ["-c", f'sandbox_workspace_write.writable_roots=["{review_root}"]']
        # reasoning effort is a codex config key (minimal|low|medium|high|xhigh).
        effort = spec.get("codex_effort")
        if effort:
            argv += ["-c", f'model_reasoning_effort="{effort}"']
        # codex_model is the codex-only model slug — kept distinct from `model` (the
        # claude model) so the two tools don't collide on one shared key.
        model = spec.get("codex_model") or spec.get("model")
        if model:
            argv += ["-m", model]
        return argv + extra
    argv = ["claude"]  # claude (default)
    if spec.get("model"):
        argv += ["--model", spec["model"]]
    if spec.get("effort"):
        argv += ["--effort", spec["effort"]]
    return argv + extra


def author_argv():
    return _build_argv(cfg()["author"], "author")


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
        if not _REPO and os.environ.get("DUAL_AUTHOR_NS"):
            # dashboard/collector pane: cwd is outside the repo, but the ns is
            # pinned via env, so read the repo `register` recorded at dispatch.
            # (Guarded on DUAL_AUTHOR_NS: without it base() would recurse into
            # ns() -> _repo().) Enables PR polling + tab labels from any pane.
            try:
                with open(os.path.join(base(), "repo.txt")) as f:
                    _REPO = f.read().strip()
            except OSError:
                pass
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
    r = _repo()
    if r:  # record owner/repo so panes outside the repo (dashboard) can resolve it
        try:
            with open(os.path.join(base(), "repo.txt"), "w") as f:
                f.write(r)
        except OSError:
            pass


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


_PR_CACHE = {}  # issue -> (last_check_ts, info_dict|None); a MERGED info is terminal


def pr_info(issue):
    """PR facts for branch issue/<N> — {number,url,state,draft,checks:{ok,fail,
    pending}} or None. Polled at most every timeouts.pr_poll_secs per issue;
    MERGED is terminal and never re-polled. Feeds both the merge ground truth
    and the dashboard's PR/checks columns."""
    now = time.time()
    ts, info = _PR_CACHE.get(issue, (0.0, None))
    if info and info.get("state") == "MERGED":
        return info
    if now - ts < cfg()["timeouts"]["pr_poll_secs"]:
        return info
    repo = _repo()
    if repo:
        out = sh("gh", "pr", "list", "--repo", repo, "--head", f"issue/{issue}",
                 "--state", "all", "--limit", "1", "--json",
                 "number,url,state,isDraft,statusCheckRollup")
        try:
            lst = json.loads(out)
            if lst:
                p = lst[0]
                ok = fail = pend = 0
                for chk in p.get("statusCheckRollup") or []:
                    # CheckRun rows carry status/conclusion; StatusContext rows carry state
                    concl = (chk.get("conclusion") or chk.get("state") or "").upper()
                    status = (chk.get("status") or "").upper()
                    if concl in ("SUCCESS", "NEUTRAL", "SKIPPED"):
                        ok += 1
                    elif concl in ("FAILURE", "ERROR", "CANCELLED", "TIMED_OUT",
                                   "ACTION_REQUIRED", "STALE"):
                        fail += 1
                    elif status in ("QUEUED", "IN_PROGRESS") or concl in ("", "PENDING", "EXPECTED"):
                        pend += 1
                info = {"number": p.get("number"), "url": p.get("url"),
                        "state": p.get("state"), "draft": p.get("isDraft"),
                        "checks": {"ok": ok, "fail": fail, "pending": pend}}
            else:
                info = None
        except Exception:
            pass  # keep the previous info on a flaky gh call
    _PR_CACHE[issue] = (now, info)
    return info


def pr_merged(issue):
    info = pr_info(issue)
    return bool(info and info.get("state") == "MERGED")


_DEP_CACHE = {}  # issue -> (last_check_ts, [blocking issue numbers as str])


def blocked_by(issue):
    """Issue numbers this issue is blocked by — GitHub's native issue
    dependencies (REST /dependencies/blocked_by), falling back to 'blocked by
    #N' / 'depends on #N' conventions in the body. Cached 5 min per issue;
    feeds the dashboard's DAG view."""
    issue = str(issue).lstrip("#")
    now = time.time()
    ts, deps = _DEP_CACHE.get(issue, (0.0, None))
    if deps is not None and now - ts < 300:
        return deps
    deps = []
    repo = _repo()
    if repo:
        out = sh("gh", "api", f"repos/{repo}/issues/{issue}/dependencies/blocked_by",
                 "--jq", "[.[].number]")
        try:
            deps = [str(n) for n in json.loads(out)]
        except Exception:
            deps = []
        if not deps:
            body = sh("gh", "issue", "view", issue, "--repo", repo,
                      "--json", "body", "-q", ".body")
            deps = sorted({m for m in re.findall(
                r"(?:blocked.by|depends.on|requires)\s+#(\d+)", body, re.I)}, key=int)
    _DEP_CACHE[issue] = (now, deps)
    return deps


def rounds_for(issue):
    """Review rounds parsed from on-disk review files
    (<base>/issue-<N>/<tag>-<slot>[2].md) → [{"tag": "r1", "slots": {"codex":
    "FAIL", "claude": "CANCELLED"}}, ...] in round order. A file whose VERDICT
    line hasn't landed yet reports "running". Dashboard-only, read-only."""
    rd = os.path.join(base(), f"issue-{issue}")
    slots = sorted({str(r.get("slot") or r.get("tool") or i)
                    for i, r in enumerate(cfg()["review"]["reviewers"])},
                   key=len, reverse=True)  # longest first: 'codex-x' beats 'codex'
    rounds = {}
    try:
        names = os.listdir(rd)
    except OSError:
        return []
    for fn in names:
        if not fn.endswith(".md"):
            continue
        stem = fn[:-3]
        for slot in slots:
            for suffix in (f"-{slot}2", f"-{slot}"):  # '2' = claude-substituted slot
                if stem.endswith(suffix):
                    tag = stem[: -len(suffix)]
                    rounds.setdefault(tag, {})[slot] = _verdict_of(os.path.join(rd, fn)) or "running"
                    break
            else:
                continue
            break

    def _key(t):
        return ([int(x) for x in re.findall(r"\d+", t)], t)

    return [{"tag": t, "slots": rounds[t]} for t in sorted(rounds, key=_key)]


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
                         "workspace_id": (reg.get(str(n)) or {}).get("ws"),
                         "pane_id": None, "tab_id": None,
                         "pr": pr_info(n), "rounds": rounds_for(n),
                         "blocked_by": blocked_by(n), "tail": []})
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
            "tab_id": a.get("tab_id"),
            "pr": pr_info(n),
            "rounds": rounds_for(n),
            "blocked_by": blocked_by(n),
            # last screenfuls of the worker pane, for the dashboard detail panel
            "tail": [ln.rstrip() for ln in text.splitlines() if ln.strip()][-30:],
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
    has been idle for timeouts.reviewer_idle_sweep_secs is done — review file on disk
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
        _t = cfg()["timeouts"]
        grace = {"idle": _t["reviewer_idle_sweep_secs"],
                 "unknown": _t["reviewer_unknown_sweep_secs"]}.get(status)
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


def _spawn_reviewer(name, base_pane, split, cwd, spec, prompt):
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
    with _codex_auth_gate(spec.get("tool")):
        return _spawn_reviewer_inner(name, base_pane, split, cwd, spec, prompt)


def _spawn_reviewer_inner(name, base_pane, split, cwd, spec, prompt):
    _run("herdr", "agent", "start", name, "--tab", base_pane, "--split", split,
         "--no-focus", "--cwd", cwd, "--", *_build_argv(spec, "review"), prompt)
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
        argv = " ".join(shlex.quote(a) for a in _build_argv(spec, "review"))
        f.write(f"#!/bin/zsh\ncd {shlex.quote(cwd)}\nexec {argv} {shlex.quote(prompt)}\n")
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
    # <base>/codex-down sentinel (manual skip): an operator drops this when codex is
    # quota-dead at OpenAI so rounds don't waste a spawn attempt on it — the codex
    # SLOT is filled with a second independent claude instead, so every round still
    # gets two reviewers. The result key stays "codex" (callers read it structurally);
    # "tool" records the substitution. The flag SELF-EXPIRES after timeouts.codex_down_ttl_secs
    # so a forgotten sentinel can't silently pin every round to dual-claude forever;
    # once expired we delete it and attempt codex again (the spawn-failure fallback
    # below still covers the case where codex is genuinely still down).
    cd_path = os.path.join(base(), "codex-down")
    codex_down = False
    try:
        if time.time() - os.path.getmtime(cd_path) < cfg()["timeouts"]["codex_down_ttl_secs"]:
            codex_down = True
        else:
            os.remove(cd_path)  # stale flag — let codex be retried
    except OSError:
        pass  # no sentinel (or it vanished) → attempt codex normally
    # The panel is config-driven: any mix/count of codex+claude reviewers. Split
    # placement cycles right/down. A claude spec is kept aside to substitute into any
    # codex slot that's forced down (sentinel) or fails to spawn — so every round
    # still yields a decided dual (or N-) reviewer verdict.
    reviewers = cfg()["review"]["reviewers"]
    splits = ["right", "down"]
    claude_sub = copy.deepcopy(next((r for r in reviewers if r.get("tool") == "claude"),
                                    {"tool": "claude", "model": "sonnet", "effort": "high"}))

    def _mk_prompt(outfile):
        return (f"{prompt} Write your FULL review to {outfile}, ending the file "
                f"with VERDICT: PASS or VERDICT: FAIL on its own line.")

    for i, rv in enumerate(reviewers):
        slot = str(rv.get("slot") or rv.get("tool") or f"r{i}")
        split = splits[i % len(splits)]
        spec = copy.deepcopy(rv)
        name, outfile = f"{worker}-{slot}-{tag}", f"{rd}/{tag}-{slot}.md"
        if spec.get("tool") == "codex" and codex_down:  # manual skip → claude sub
            spec = copy.deepcopy(claude_sub)
            name, outfile = f"{worker}-{slot}-{tag}x", f"{rd}/{tag}-{slot}2.md"
        plan[slot] = {"name": name, "file": outfile, "spec": spec,
                      "tool": spec.get("tool"), "prompt": _mk_prompt(outfile),
                      "split": split, "pane": None}
    results = {}
    try:
        for slot, p in plan.items():  # SPAWN + VERIFY (one retry)
            p["pane"] = (_spawn_reviewer(p["name"], base_pane, p["split"], cwd, p["spec"], p["prompt"])
                         or _spawn_reviewer(p["name"], base_pane, p["split"], cwd, p["spec"], p["prompt"]))
            # Self-healing codex fallback: if a codex reviewer can't start (quota
            # dead, or it lost the auth-lock race after both retries) substitute a
            # fresh claude into the slot for THIS round, so we still get a full panel
            # and a DECIDED round instead of an undecided SPAWN-FAILED slot. No
            # sentinel needed — codex is attempted from scratch next round, so the
            # moment it recovers it's used again automatically.
            if p["pane"] is None and p["tool"] == "codex":
                p["spec"] = copy.deepcopy(claude_sub)
                p["tool"] = "claude"
                p["name"] = f"{worker}-{slot}-{tag}x"
                p["file"] = f"{rd}/{tag}-{slot}2.md"
                p["prompt"] = _mk_prompt(p["file"])
                p["pane"] = (_spawn_reviewer(p["name"], base_pane, p["split"], cwd, p["spec"], p["prompt"])
                             or _spawn_reviewer(p["name"], base_pane, p["split"], cwd, p["spec"], p["prompt"]))
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


def _push_event(state, issue, text, now):
    ev = state.setdefault("_events", [])
    ev.append({"ts": now, "issue": str(issue), "text": text})
    del ev[:-80]  # rolling feed for the dashboard's activity panel


def update_timing(state, rows, record=False):
    # record=True (watch/dashboard only, so concurrent wait processes don't
    # double-log) also appends phase transitions / verdicts / needs-input to the
    # persisted _events feed the dashboard renders.
    now = time.time()
    for r in rows:
        st = state.setdefault(str(r["issue"]), {})
        if "done" in st and not r["verdict"]:  # fresh run of a previously finished issue
            st.clear()
        st.setdefault("start", now)
        if r["phase"] != st.get("phase"):
            if record and st.get("phase"):
                _push_event(state, r["issue"], f"{st['phase']} → {r['phase']}", now)
            st["phase"] = r["phase"]
            st["phase_start"] = now
        if r["verdict"] and "done" not in st:
            st["done"] = now
            if record:
                _push_event(state, r["issue"], "verdict — finished", now)
        if record:
            if r["input"] and not st.get("input_seen"):
                st["input_seen"] = True
                _push_event(state, r["issue"], "NEEDS INPUT", now)
            elif not r["input"]:
                st.pop("input_seen", None)
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


def apply_labels(rows, labels):
    """Live-rename the herdr surfaces for each issue so every page identifies the
    work: workspace + agent get the icon-led status title (issue · phase), and the
    issue's TAB gets `#<N> · <repo-name>` (static — the tab strip otherwise shows a
    bare tab number). `labels` caches the last value set to avoid rename churn."""
    repo = _repo()
    short = repo.split("/")[-1] if repo else ns()
    for r in rows:
        label = f"{row_icon(r)} {ns()}-issue-{r['issue']} · {r['phase']}"
        ws = r.get("workspace_id")
        if ws and labels.get(("ws", ws)) != label:
            sh("herdr", "workspace", "rename", ws, label)
            labels[("ws", ws)] = label
        pane = r.get("pane_id")  # worker still live → its agents-page title
        if pane and labels.get(("agent", pane)) != label:
            sh("herdr", "agent", "rename", pane, label)
            labels[("agent", pane)] = label
        tab = r.get("tab_id")
        tab_label = f"#{r['issue']} · {short}"
        if tab and labels.get(("tab", tab)) != tab_label:
            sh("herdr", "tab", "rename", tab, tab_label)
            labels[("tab", tab)] = tab_label


POLL_CHOICES = (5, 20, 60)


def poll_interval():
    """Dashboard/collector polling cadence in seconds — read from
    <base>/poll-interval (written by the dashboard's [p] key, persists across
    runs). Anything absent/invalid falls back to 5."""
    try:
        with open(os.path.join(base(), "poll-interval")) as f:
            v = int(f.read().strip())
        return v if v in POLL_CHOICES else POLL_CHOICES[0]
    except (OSError, ValueError):
        return POLL_CHOICES[0]


def collect_tick(state, labels, issues_pin=(), queued_pin=()):
    """One full monitoring tick — shared by legacy `watch` and the curses
    dashboard's collector thread: discover workers, snapshot (phases, PR, rounds,
    tails), update timings + activity events, sweep finished reviewer panes, and
    apply workspace/agent/tab labels. Returns (rows, queued)."""
    ag = agents()
    active = list(issues_pin) or discover_issues(ag)
    q = list(queued_pin) or read_queue(active)
    rows = snapshot(active)
    update_timing(state, rows, record=True)
    sweep_reviewers(state, ag)
    save_state(state)
    apply_labels(rows, labels)
    return rows, q


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
    modes = ("watch", "wait", "collect", "review", "ns", "register", "unregister",
             "worker-pane", "close-reviewers", "config", "author-launch")
    if len(sys.argv) < 2 or sys.argv[1] not in modes:
        print(__doc__)
        sys.exit(2)
    mode, rest = sys.argv[1], sys.argv[2:]

    if mode == "config":
        # `config`            -> whole resolved config as JSON
        # `config a.b.c`      -> one value (scalar plain, dict/list as JSON); exit 1 if absent
        if not rest:
            print(json.dumps(cfg(), indent=2))
            return
        v = cfg_get(rest[0])
        if v is None:
            sys.exit(1)
        print(json.dumps(v) if isinstance(v, (dict, list)) else v)
        return

    if mode == "author-launch":
        # author-launch --pane <P> --prompt-file <F> [--cwd <D>]: launch the
        # configured AUTHOR agent (claude or codex) in an existing pane, running a
        # short launch script (prompt read from the file at exec time → no typing
        # truncation). Used by the dispatcher instead of a hardcoded `claude` line.
        opts = {"pane": None, "prompt-file": None, "cwd": os.getcwd()}
        args = rest
        while args:
            opts[args[0].lstrip("-")] = args[1]
            args = args[2:]
        if not opts["pane"] or not opts["prompt-file"]:
            print("author-launch requires --pane and --prompt-file", file=sys.stderr)
            sys.exit(2)
        os.makedirs(base(), exist_ok=True)
        script = os.path.join(base(), f"launch-worker-{os.getpid()}.sh")
        quoted = " ".join(shlex.quote(a) for a in author_argv())
        with open(script, "w") as f:
            f.write(f'#!/bin/zsh\ncd {shlex.quote(opts["cwd"])}\n'
                    f'exec {quoted} "$(cat {shlex.quote(opts["prompt-file"])})"\n')
        os.chmod(script, 0o755)
        sh("herdr", "pane", "run", opts["pane"], script)
        print(f"launched author ({cfg()['author']['tool']}) in {opts['pane']}")
        return

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
        opts = {"cwd": os.getcwd(),
                "timeout-mins": str(cfg()["review"]["timeout_mins"]),
                "prompt-file": None}
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
        legacy = "--legacy" in issues
        issues = [i for i in issues if i != "--legacy"]
        if not legacy and sys.stdout.isatty() and (os.environ.get("TERM") or "dumb") != "dumb":
            # Full-screen Textual dashboard (single viewport, selectable issues,
            # detail panel, [g] pipeline-graph view), run via `uv run` — uv
            # provisions python>=3.10 + textual in a cached env on first use.
            # --legacy / non-TTY / no uv falls back to the plain-text render.
            uv = None
            for cand in [os.path.expanduser("~/.local/bin/uv"), "/opt/homebrew/bin/uv"]:
                if os.path.exists(cand):
                    uv = cand
                    break
            uv = uv or (subprocess.run(["which", "uv"], capture_output=True,
                                       text=True).stdout.strip() or None)
            if uv:
                dash = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.py")
                argv = [uv, "run", "--quiet", dash, *issues]
                if queued:
                    argv += ["--queued", ",".join(queued)]
                os.execv(uv, argv)  # env (DUAL_AUTHOR_NS) is inherited
            sys.stderr.write("[dual-author] uv not found; using plain-text watch\n")
        labels = {}  # (kind, id) -> last label set, to avoid rename churn
        while True:
            state = load_state()
            rows, q = collect_tick(state, labels, issues, queued)
            print("\033[2J\033[H" + render(rows, state, q), flush=True)
            time.sleep(5)
    elif mode == "collect":
        # Headless data loop for the Textual dashboard, run as a SEPARATE
        # PROCESS so the UI can never be blocked by herdr/gh calls (in a
        # thread, the GIL is held during every subprocess spawn — dozens per
        # tick — which visibly starved the UI). Writes an atomic JSON snapshot
        # each tick; `touch <base>/poll-now` forces an immediate re-poll; exits
        # on its own when the parent dashboard process dies.
        labels = {}
        parent = os.environ.get("DUAL_AUTHOR_COLLECT_PPID")
        snap_path = os.path.join(base(), "dashboard.json")
        poll_now = os.path.join(base(), "poll-now")
        while True:
            state = load_state()
            err, rows, q, qdeps = None, [], [], {}
            try:
                rows, q = collect_tick(state, labels, issues, queued)
                qdeps = {n: blocked_by(n) for n in q}
            except Exception as e:  # surface in the UI instead of dying
                err = f"{type(e).__name__}: {e}"
            os.makedirs(base(), exist_ok=True)
            tmp = f"{snap_path}.{os.getpid()}.tmp"
            try:
                with open(tmp, "w") as f:
                    json.dump({"ts": time.time(), "rows": rows, "queue": q,
                               "qdeps": qdeps, "state": state, "err": err,
                               "repo": _repo()}, f)
                os.replace(tmp, snap_path)
            except OSError:
                pass
            waited = 0
            while waited < poll_interval():  # re-read each second: [p] in the
                # dashboard changes the cadence mid-sleep (60 → 5 shouldn't
                # keep sleeping a full minute)
                if parent and (os.getppid() == 1 or str(os.getppid()) != parent):
                    sys.exit(0)  # dashboard is gone — don't linger as an orphan
                if os.path.exists(poll_now):
                    with contextlib.suppress(OSError):
                        os.remove(poll_now)
                    break
                time.sleep(1)
                waited += 1
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
