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
  monitor.py base-branch                                     print the branch lanes are cut from / PRs merge into
  monitor.py author-launch --pane <P> --prompt-file <F> [--cwd <D>]
                                                             launch the configured AUTHOR agent (claude|codex) in a pane
  monitor.py set-root [<path>]                               record the primary checkout path (auto-dispatch/
                                                             recycle need it; run once at setup, from the repo)
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
        EVERY reviewer runs to its own conclusion: a FAIL no longer cancels the
        others (that reduced the panel to its fastest member — see review_round).
        Prints JSON {codex:{file,verdict}, claude:{file,verdict}} and
        exits 0 (verdicts PASS/FAIL/MISSING/SPAWN-FAILED). Reviews land in
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

watch/collect also own the WORKSPACE LIFECYCLE (config [lifecycle], both on by
default — previously the dispatcher LLM's job, which stalled whenever that session
was paused/buried): recycle = when an issue's PR is MERGED (gh ground truth), close
its panes, unregister, remove the worktree workspace, delete the local branch;
dispatch = when active issues < dispatch.parallel and queue.txt has entries, cut a
worktree off fresh main, launch the configured author with the issue brief
($BASE/issue-N-brief.txt if the dispatcher wrote one, else generated from the issue
title/body), register it, and mark the issue in-progress (label + board Status).
Requires `set-root` to have been run. Serialized by a lock so two watchers can't
double-dispatch; a queue head that fails to dispatch 3x is dropped with an event.

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
# (claude OR codex OR grok), the review panel (any mix of the three, models, effort),
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
    # parallel: issues in flight. respect_dependencies: skip a queued issue while
    # it still has an OPEN blocker (native GitHub issue dependencies ∪ "blocked by
    # #N" in the body) and dispatch the next unblocked entry instead — the held
    # entry stays in queue.txt. Fails open on any gh problem; see open_blockers().
    # base_branch: the branch lanes are cut from and PRs merge into; "" = the repo's
    # default branch. See lane_base(). skip_labels: a queued issue carrying one is
    # popped, never dispatched (as is a PR or a CLOSED issue); see skip_reason().
    "dispatch": {"parallel": 3, "respect_dependencies": True, "base_branch": "",
                 "dependency_fail_closed": False,
                 "skip_labels": ["epic", "owner-step", "manual", "parked"]},
    "review": {
        # 15 was tight once a reviewer could no longer be short-circuited: a 69k diff
        # at high reasoning effort does not reliably reach a written verdict inside
        # it, and the slot resolves MISSING with its findings thrown away. Raised
        # 2026-10-07 alongside the write-the-file-first budget in _mk_prompt.
        "timeout_mins": 25,
        # The review panel. Order sets split placement (right, down, …). Each entry:
        # slot (stable id used in file/agent names), tool (codex|claude|grok), and
        # optional model/effort/extra_args (+ codex_sandbox/codex_approval, or
        # grok_model/grok_effort/grok_sandbox).
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
    # Deterministic pane/workspace lifecycle, owned by the MONITOR (watch/collect)
    # instead of the dispatcher LLM — the LLM being paused/buried must not stall
    # the pipeline. recycle: when an issue's PR is MERGED (gh ground truth, never
    # pane text), close its reviewer panes, unregister it, remove its worktree
    # workspace, and delete the local branch. dispatch: when active < dispatch.parallel
    # and queue.txt is non-empty, create the next issue's worktree, launch the
    # configured author, register it, and mark the issue in-progress (label+board).
    "lifecycle": {"recycle": True, "dispatch": True},
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
        # A worker runs in a linked worktree, where an untracked .dual-author.toml
        # does not exist — it lives only in the primary checkout. Read the primary's
        # first so a worker resolves the same base_branch/merge policy as the
        # dispatcher; a file in the worktree itself still wins.
        common = sh("git", "rev-parse", "--path-format=absolute",
                    "--git-common-dir").strip()
        if common and os.path.basename(common) == ".git":
            primary = os.path.dirname(common)
            if os.path.realpath(primary) != os.path.realpath(root):
                paths.append(os.path.join(primary, ".dual-author.toml"))
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
    if tool == "grok":
        # grok runs as an interactive TUI in a pane like claude/codex (herdr knows
        # kind=grok), and takes its prompt as a positional arg, so the launch script
        # needs no typing. --always-approve is the only non-interactive permission
        # switch: without it the reviewer stalls on its first tool-use dialog and the
        # round reads as a timeout. Left on by default for the same reason codex
        # reviewers pass --ask-for-approval never.
        argv = ["grok"]
        if spec.get("grok_approve", True):
            argv += ["--always-approve"]
        if spec.get("grok_trust", True):
            # FOLDER TRUST IS A SEPARATE GATE from --always-approve, and it is the
            # one that actually stops a reviewer: grok opens on "Do you trust the
            # contents of this directory?" and waits. Every lane is a fresh worktree,
            # so every lane asks, and herdr reports the blocked agent as "idle" —
            # which _agent_alive accepts as alive, so the slot never falls back to a
            # claude substitute. It sits until the round's timeout and reports
            # MISSING. --trust grants and records the folder in
            # ~/.grok/trusted_folders.toml at launch. It is undocumented in --help
            # on 1.0.46 but works (owner-approved 2026-10-07).
            argv += ["--trust"]
        sandbox = spec.get("grok_sandbox")  # unset = grok's own default profile
        if sandbox:
            argv += ["--sandbox", sandbox]
        # --reasoning-effort is a free-form string at the CLI (low|medium|high|xhigh
        # for grok-4.7); an unknown value is rejected at startup, which kills the
        # slot, so verify a new one with `grok -p ok --effort <v>` before setting it.
        effort = spec.get("grok_effort") or spec.get("effort")
        if effort:
            argv += ["--effort", effort]
        # grok_model mirrors codex_model: tool-scoped so the three tools don't collide
        # on one shared `model` key. `grok models` lists what this account can use.
        model = spec.get("grok_model") or spec.get("model")
        if model:
            argv += ["-m", model]
        return argv + extra
    # launch_prefix replaces the binary: `teamclaude run --auto-fallback --` runs claude
    # through the pooled-account proxy, which is the only way past a weekly limit on THIS
    # machine's own Claude account (a bare `claude` then starts, prints "You've hit your
    # weekly limit", and sits on a dialog — reported as SPAWN-FAILED). The wrapper implies
    # the claude binary, so the prefix stands in for it rather than preceding it.
    argv = list(spec.get("launch_prefix") or ["claude"])
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
        # herdr 0.9.0 dropped `name` from `agent list`. Keying on name alone collapsed every
        # agent onto "", so only the LAST survived: workers read as missing, and the reaper
        # could mistake a worker for a straggler reviewer. Fall back to a unique handle.
        return {a.get("name") or a.get("pane_id") or a.get("terminal_id") or str(i): a
                for i, a in enumerate(d["result"]["agents"])}
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


def branch_for(issue):
    """The lane's ACTUAL branch — `issue/<N>` unless the lane recorded another.

    A re-dispatched issue cannot always use `issue/<N>`: if an earlier lane already
    merged a PR from that branch (a split deliverable landing as PR 1 of 2, say), the
    merge ground truth in `pr_info` finds that old MERGED PR and recycles the fresh
    lane on its first tick, forever. Dispatch therefore picks a distinct branch in that
    case and records it here, and every branch-keyed operation reads it back."""
    e = load_registry().get(str(issue)) or {}
    return e.get("branch") or f"issue/{issue}"


def register(issue, workspace, pane, branch=None):
    reg = load_registry()
    reg[str(issue)] = {"ws": workspace, "term": _terminal_of(pane), "root_pane": pane,
                       "branch": branch or f"issue/{issue}"}
    save_registry(reg)
    r = _repo()
    if r:  # record owner/repo so panes outside the repo (dashboard) can resolve it
        try:
            with open(os.path.join(base(), "repo.txt"), "w") as f:
                f.write(r)
        except OSError:
            pass
    set_root()  # refresh the primary-checkout path when resolvable from cwd


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
        out = sh("gh", "pr", "list", "--repo", repo, "--head", branch_for(issue),
                 "--state", "all", "--limit", "1", "--json",
                 "number,url,state,isDraft,baseRefName,statusCheckRollup")
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
                        "base": p.get("baseRefName"),
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

# The body convention for dependencies — ONE parser, two callers: blocked_by()
# (the dashboard's [g] DAG) and open_blockers() (the dispatch gate). Matches
# "blocked by #12", "Blocked by: #12", "blocked-by #12", "depends on #12",
# "requires #12" case-insensitively, several per line, plus a comma/and-separated
# run after a single keyword ("blocked by #12, #13 and #14").
DEP_KEY_RE = re.compile(r"(?:blocked[\s._-]*by|depends[\s._-]*on|requires)\s*:?\s*", re.I)
DEP_REF_RE = re.compile(r"\s*(?:,|;|&|\+|and\b)?\s*#(\d+)", re.I)


def deps_in_body(body):
    """Issue numbers referenced by the 'blocked by #N' body convention — deduped,
    numeric order. A reference must directly follow the keyword (optionally
    through ',' / 'and'), so prose like 'blocked by the change in #12' is not a
    dependency."""
    found = set()
    for m in DEP_KEY_RE.finditer(body or ""):
        pos = m.end()
        while True:
            r = DEP_REF_RE.match(body, pos)
            if not r:
                break
            found.add(r.group(1))
            pos = r.end()
    return sorted(found, key=int)


def blocked_by(issue):
    """Issue numbers this issue is blocked by — GitHub's native issue
    dependencies (REST /dependencies/blocked_by), falling back to 'blocked by
    #N' / 'depends on #N' conventions in the body. Cached 5 min per issue;
    feeds the dashboard's DAG view. State-agnostic (a closed blocker is still
    listed) — the dispatch gate needs open-only, see open_blockers()."""
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
            deps = deps_in_body(body)
    _DEP_CACHE[issue] = (now, deps)
    return deps


# ---- dispatch dependency gate ----------------------------------------------
# Auto-dispatch pops queue.txt FIFO; without this gate it starts a worker against
# an unmerged prerequisite, so the branch is cut off a base that lacks what it
# depends on (conflicts, or an implementation against the wrong contract) and the
# only defence was hand-gating queue.txt, which no unattended run can do.
# The gate answers "does this issue have an OPEN blocker?" from BOTH sources and
# FAILS OPEN on every gh problem: a monitoring convenience must never be able to
# wedge the pipeline. dispatch.respect_dependencies = false disables it entirely.
DEP_GATE_TTL = 60          # per-issue cache — the gate runs every dispatch tick
DEP_GATE_TIMEOUT = 10      # secs per gh call; a hung gh must not stall a tick
DEP_GATE_TICK_BUDGET = 10  # secs of LIVE lookups per tick; then the scan defers

_GATE_CACHE = {}   # issue -> (last_check_ts, [OPEN blocker numbers as str])
_STATE_CACHE = {}  # issue -> (last_check_ts, "open"/"closed"/"" unknown)
_GATE_WARNED = set()  # issues whose lookup already reported a gh failure (log once)


def _gh(*args, timeout=DEP_GATE_TIMEOUT):
    """gh with a hard timeout → (ok, stdout). ok=False on a missing binary, a
    non-zero exit (unauthenticated, rate-limited, 404 on a repo without the
    dependencies API) or a timeout — callers read that as "unknown" and fail
    open. Unlike sh(), which returns "" for both success-with-no-output and
    failure, this keeps the two apart."""
    try:
        r = subprocess.run(("gh",) + args, capture_output=True, text=True,
                           timeout=timeout)
    except Exception:  # FileNotFoundError, TimeoutExpired, OSError …
        return False, ""
    return (r.returncode == 0), (r.stdout if r.returncode == 0 else "")


def _issue_open(issue):
    """True if <issue> is OPEN. Unknown (gh failed, or the number is a PR rather
    than an issue) → False, so an unresolvable body reference never holds the
    queue. Cached DEP_GATE_TTL secs."""
    issue = str(issue)
    now = time.time()
    ts, st = _STATE_CACHE.get(issue, (0.0, None))
    if st is None or now - ts >= DEP_GATE_TTL:
        repo = _repo()
        ok, out = (_gh("issue", "view", issue, "--repo", repo, "--json", "state",
                       "-q", ".state") if repo else (False, ""))
        st = out.strip().lower() if ok else ""
        _STATE_CACHE[issue] = (now, st)
    return st == "open"


def open_blockers(issue, deadline=None, state=None):
    """OPEN blockers of <issue> — the UNION of GitHub's native issue dependencies
    (REST .../dependencies/blocked_by, entries whose .state is "open"; there is no
    GraphQL equivalent) and deps_in_body() references that are still open. []
    means dispatchable.

    Fails open: gh missing/unauthenticated/rate-limited/timed out, a repo without
    the dependencies API, unparseable JSON — each unresolved source contributes
    nothing, and the failure is reported ONCE per issue (activity feed when a
    `state` is given, else stderr). Cached DEP_GATE_TTL secs per issue.
    dispatch.dependency_fail_closed = true inverts that for runs where dispatching
    over an open blocker is worse than stalling: an unresolved lookup then returns
    None (undetermined), the tick holds, and nothing is cached.
    `deadline` bounds live lookups per tick: past it, an uncached issue returns
    None ("not determined"), which the caller reads as "don't dispatch this one
    yet" and retries next tick against a warm cache."""
    issue = str(issue).lstrip("#")
    now = time.time()
    ts, blk = _GATE_CACHE.get(issue, (0.0, None))
    if blk is not None and now - ts < DEP_GATE_TTL:
        return blk
    if deadline is not None and now > deadline:
        return None
    repo = _repo()
    blk, failed = [], not repo
    if repo:
        ok, out = _gh("api", f"repos/{repo}/issues/{issue}/dependencies/blocked_by",
                      "--jq", '[.[]|select(.state=="open")|.number]')
        if ok:
            try:
                blk = [str(n) for n in json.loads(out or "[]")]
            except Exception:
                failed = True
        else:
            failed = True
        ok, body = _gh("issue", "view", issue, "--repo", repo, "--json", "body",
                       "-q", ".body")
        if ok:
            for d in deps_in_body(body):
                if d not in blk and _issue_open(d):
                    blk.append(d)
        else:
            failed = True
    fail_closed = bool(cfg()["dispatch"].get("dependency_fail_closed", False))
    if failed and issue not in _GATE_WARNED:
        _GATE_WARNED.add(issue)
        msg = ("⛔ dependency check unavailable (gh) — holding (fail-closed)"
               if fail_closed else
               "⚠ dependency check unavailable (gh) — dispatching unguarded")
        if state is not None:
            _push_event(state, issue, msg, now)
        else:
            sys.stderr.write(f"[dual-author] #{issue}: {msg}\n")
    if failed and fail_closed and not blk:
        # dispatch.dependency_fail_closed: a blind gate must not dispatch. Return
        # "undetermined" (the deadline path's contract) so the caller holds the
        # tick and retries against a live lookup, and cache nothing — a proxy or
        # GitHub blip once dispatched three issues over their open blockers.
        return None
    _GATE_CACHE[issue] = (now, blk)
    return blk


# ---- dispatch skip guard ---------------------------------------------------
# A board spans epics and owner-only steps, and a queue fed from it can carry
# them; dispatching one puts an agent on work that is not a lane (an epic) or
# not an agent's to do (an owner-step). dispatch.skip_labels names those
# labels. A queued entry carrying one, a PR number, or a CLOSED issue is POPPED
# (unlike a blocked entry, it will never become dispatchable) with one feed
# line. Same caching, tick budget and fail-open contract as open_blockers():
# a gh failure dispatches as before and says so once per issue;
# dispatch.dependency_fail_closed makes a blind lookup hold the tick instead.
_META_CACHE = {}      # issue -> (last_check_ts, {"labels","state","pr"}; {} unknown)
_META_WARNED = set()  # issues whose lookup already reported a gh failure


def skip_reason(issue, deadline=None, state=None):
    """Why <issue> must not be dispatched ("labelled epic (not for dual-author)",
    a PR, not open) → str; "" when it may go; None when undetermined (tick
    budget spent on a cold cache, or a blind lookup under
    dependency_fail_closed). One `gh api repos/{R}/issues/{N}` call, cached
    DEP_GATE_TTL secs."""
    issue = str(issue).lstrip("#")
    now = time.time()
    ts, meta = _META_CACHE.get(issue, (0.0, None))
    if meta is None or now - ts >= DEP_GATE_TTL:
        if deadline is not None and now > deadline:
            return None
        repo = _repo()
        ok, out = (_gh("api", f"repos/{repo}/issues/{issue}", "--jq",
                       "{labels:[.labels[].name],state:.state,"
                       "pr:(.pull_request != null)}") if repo else (False, ""))
        meta = {}
        if ok:
            try:
                d = json.loads(out)
                meta = {"labels": [str(x) for x in d.get("labels") or []],
                        "state": str(d.get("state") or "").lower(),
                        "pr": bool(d.get("pr"))}
            except Exception:
                meta = {}
        if not meta:
            fail_closed = bool(cfg()["dispatch"].get("dependency_fail_closed", False))
            if issue not in _META_WARNED:
                _META_WARNED.add(issue)
                msg = ("⛔ label check unavailable (gh): holding (fail-closed)"
                       if fail_closed else
                       "⚠ label check unavailable (gh): dispatching unguarded")
                if state is not None:
                    _push_event(state, issue, msg, now)
                else:
                    sys.stderr.write(f"[dual-author] #{issue}: {msg}\n")
            if fail_closed:
                return None  # cache nothing; retry live next tick
        _META_CACHE[issue] = (now, meta)
    if not meta:
        return ""  # unknown → fail open
    if meta["pr"]:
        return "is a pull request, not an issue (not for dual-author)"
    if meta["state"] and meta["state"] != "open":
        return f"issue is {meta['state']} (nothing to dispatch)"
    skip = cfg()["dispatch"].get("skip_labels") or []
    skip = {str(s).casefold() for s in ([skip] if isinstance(skip, str) else skip)}
    hit = [lb for lb in meta["labels"] if lb.casefold() in skip]
    if hit:
        return f"labelled {', '.join(hit)} (not for dual-author)"
    return ""


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


def diff_stats(cwd):
    """Total lines +added/-removed by the issue branch in its worktree —
    committed AND uncommitted tracked changes, measured against the merge-base
    with the base branch (dispatch.base_branch, else the default branch). None
    when unresolvable (no cwd, worktree gone, detached repo state); shown as the
    +/- column in the dashboard."""
    if not cwd or not os.path.isdir(cwd):
        return None
    try:
        # A lane cut from a non-default base measured against origin/HEAD would
        # count every commit that base carries over main as the lane's own.
        pinned = cfg_get("dispatch.base_branch")
        refs = ((f"origin/{pinned}",) if pinned else ()) + (
            "origin/HEAD", "origin/main", "origin/master", "main", "master")
        for ref in refs:
            mb = _run("git", "-C", cwd, "merge-base", "HEAD", ref, timeout=10)
            if mb.returncode == 0:
                break
        else:
            return None
        d = _run("git", "-C", cwd, "diff", "--numstat", mb.stdout.strip(), timeout=10)
        if d.returncode != 0:
            return None
        add = rem = 0
        for ln in d.stdout.splitlines():
            cols = ln.split("\t")
            if len(cols) >= 2 and cols[0].isdigit() and cols[1].isdigit():
                add += int(cols[0])
                rem += int(cols[1])
        return {"add": add, "del": rem}
    except Exception:
        return None


def _phase_from_file(issue):
    """The phase a worker last RECORDED to its phase file, or None.

    The pane-text marker (`echo "[dual-author] phase: X ::"`) only reaches the monitor
    for a CODEX worker, whose echo lands in plain terminal output. A claude worker's
    echo is a collapsed tool line that its TUI truncates, and `herdr pane read` does
    not reproduce it: verified 2026-10-01 on three Opus-authored lanes — zero markers
    in the last 900 lines while all three were hours into review rounds. Authoring moved
    to Opus 5.5 the day before, which is exactly when every lane's phase froze at
    "starting". A file is immune to how any agent TUI renders its tool calls."""
    try:
        with open(os.path.join(base(), f"issue-{issue}-phase")) as f:
            lines = [ln.strip() for ln in f if ln.strip()]
    except OSError:
        return None
    if not lines:
        return None
    tok = PHASE_TOKEN_RE.match(re.sub(r"\s+", "", lines[-1]))
    return tok.group(0) if tok else None


def snapshot(issues):
    ag = agents()
    reg = load_registry()
    # Read once, for the phase latch below: a marker that has scrolled out of the pane
    # window must not look like a return to "starting".
    prev = load_state()
    rows = []
    for n in issues:
        a = _worker_agent(ag, n, reg)
        if not a:
            # a vanished agent whose PR merged FINISHED — report verdict, not missing
            rows.append({"issue": n, "status": "missing", "phase": "-",
                         "verdict": pr_merged(n), "input": False,
                         "workspace_id": (reg.get(str(n)) or {}).get("ws"),
                         "pane_id": None, "tab_id": None, "diff": None,
                         "pr": pr_info(n), "rounds": rounds_for(n),
                         "blocked_by": blocked_by(n), "tail": []})
            continue
        text = sh("herdr", "pane", "read", a["pane_id"], "--source", "recent-unwrapped", "--lines", "120")
        # phases are single hyphenated tokens; the agent TUI hard-wraps mid-word.
        # Prefer the " ::"-sentinel form (exact through wrapping); fall back to
        # stripping ALL whitespace from the window and keeping the leading token.
        # The file is authoritative when present; pane text is the codex-era fallback.
        phases = [] if _phase_from_file(n) is None else [_phase_from_file(n)]
        for p in [] if phases else PHASE_SENT_RE.findall(text):
            tok = PHASE_TOKEN_RE.match(re.sub(r"\s+", "", p))
            if tok:
                phases.append(tok.group(0))
        if not phases:
            for p in PHASE_RE.findall(text):
                tok = PHASE_TOKEN_RE.match(re.sub(r"\s+", "", p))
                if tok:
                    phases.append(tok.group(0))
        if phases:
            phase = phases[-1]
        else:
            # No marker in THIS window does not mean the lane went back to the start.
            # The window is the last 120 lines of pane text and a worker emits a phase
            # marker ONCE, on entry — so a dense transcript scrolls it out within
            # minutes. Claude-authored lanes (the owner moved authoring to Opus 5.5 on
            # 2026-09-30) are far denser than codex ones and lose it almost at once.
            # Regressing to "starting" then made every lane read as stuck in its first
            # phase for hours, and update_timing saw a phase CHANGE, so it logged a
            # bogus "<phase> → starting" transition and reset phase_start — which also
            # destroyed the "in phase" duration the dashboard shows.
            # A phase marker is a latch: when the window no longer shows one, keep the
            # last phase this issue was known to be in.
            phase = (prev.get(str(n)) or {}).get("phase") or "starting"
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
            "diff": diff_stats(a.get("cwd")),
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
# RUN polls every reviewer concurrently and each one runs to its own conclusion; a
# FAIL does not cancel the others. Every written review goes back to the worker (the
# author) to fix against.

def _run(*args, timeout=None):
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


# herdr 0.9.0 no longer resolves `agent get/wait <name>` — only pane ids (and unique
# names it no longer reports). Reviewers are therefore tracked by the pane id the runner
# itself created, recorded here at spawn; every name-keyed lookup routes through it.
_NAME_PANE = {}


def _target(name):
    return _NAME_PANE.get(name, name)


def _agent_pane(name):
    try:
        d = json.loads(sh("herdr", "agent", "get", _target(name)))
        return d["result"]["agent"]["pane_id"]
    except Exception:
        return None


def _agent_alive(name):
    """Registered AND actually running (working/idle) — a renamed bare shell is
    'unknown' and does not count."""
    try:
        d = json.loads(sh("herdr", "agent", "get", _target(name)))["result"]["agent"]
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


def _tab_of_pane(pane):
    """Resolve a pane id to its tab id for `herdr agent start --tab`.

    `--tab` wants a TAB id. Passing a pane id makes herdr answer
    `agent_placement_not_found` and the slot is recorded SPAWN-FAILED, which
    reads like host exhaustion but is purely an addressing bug: every reviewer
    in the round fails identically, including the claude substitution that is
    supposed to be immune to the codex auth race.
    """
    if not pane:
        return pane
    try:
        listing = json.loads(sh("herdr", "pane", "list"))["result"]
        panes = listing.get("panes", listing)
        for entry in panes if isinstance(panes, list) else []:
            if entry.get("pane_id") == pane and entry.get("tab_id"):
                return entry["tab_id"]
    except Exception:
        pass
    return pane


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
    # herdr 0.9.0 removed `agent start --tab/--split/--cwd` (it now only starts an agent
    # in an EXISTING pane), so the old primary always failed and cost ~12s before this
    # path ran. Launch straight into a fresh split via a script file instead.
    try:
        pane = json.loads(sh("herdr", "pane", "split", base_pane, "--direction", split, "--no-focus"))["result"]["pane"]["pane_id"]
    except Exception:
        return None
    _NAME_PANE[name] = pane  # 0.9.0 cannot resolve the name — track by the pane we made
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
    """True when the agent reaches `status` (a state name, or an iterable of them).

    Several terminal states exist: codex settles on "done" where claude settles on
    "idle", so waiting on "idle" alone strands a finished codex reviewer until the
    round's deadline. `--until` repeats, so pass ("idle", "done") to mean either.
    """
    # herdr 0.9.0 renamed `--status` to `--until` and only resolves pane-id targets.
    states = (status,) if isinstance(status, str) else tuple(status)
    until = [a for s in states for a in ("--until", s)]
    return _run("herdr", "agent", "wait", _target(name), *until,
                "--timeout", str(timeout_ms)).returncode == 0


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
    # The hardcoded fallback carries launch_prefix because a BARE `claude` on this
    # machine hits its own weekly limit and sits on a dialog (reported SPAWN-FAILED);
    # it only applies when the roster has no claude reviewer to copy, which is now
    # the normal case.
    claude_sub = copy.deepcopy(next((r for r in reviewers if r.get("tool") == "claude"),
                                    {"tool": "claude", "model": "sonnet",
                                     "launch_prefix": ["teamclaude", "run",
                                                       "--auto-fallback", "--"]}))

    def _mk_prompt(outfile):
        # The budget clauses are not boilerplate: they are the difference between a
        # review and a MISSING slot. Measured 2026-10-07 on issue #2629, where a grok
        # reviewer reproduced BOTH of the round's real bugs, then launched an
        # unscoped full pytest run at its 47th tool call, spent ~162k tokens and hit
        # the deadline having written nothing. Its findings were real and were lost
        # entirely, twice (r4 and r4b), because the verdict file is the only contract
        # and it never got written. So: write the file FIRST and keep it current,
        # and do not run the whole suite.
        return (f"{prompt} Write your FULL review to {outfile}, ending the file "
                f"with VERDICT: PASS or VERDICT: FAIL on its own line.\n\n"
                f"BUDGET, and it is binding. Write {outfile} with a provisional "
                f"VERDICT line as soon as you have your first finding, BEFORE any "
                f"further verification, then rewrite it as you learn more. A review "
                f"you did not write down does not exist: the file is the only "
                f"contract, and a slot with no file is reported MISSING and your "
                f"findings are discarded however good they were. Never leave it "
                f"unwritten while you keep investigating.\n"
                f"Do NOT run the full test suite. The author has already run it and "
                f"its logs are in this review directory. Run at most the specific "
                f"tests touching the behaviour you are questioning, and if you are "
                f"unsure which those are, state the test you WOULD run in the review "
                f"instead of running it. Reading code to reach a conclusion beats "
                f"executing a suite to confirm it.")

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
            # Self-healing fallback for ANY non-claude slot: if the reviewer can't
            # start (codex quota dead or it lost the auth-lock race after both
            # retries; a grok login expired) substitute a fresh claude into the slot
            # for THIS round, so we still get a full panel and a DECIDED round
            # instead of an undecided SPAWN-FAILED slot. No sentinel needed — the
            # configured tool is attempted from scratch next round, so the moment it
            # recovers it's used again automatically.
            if p["pane"] is None and p["tool"] != "claude":
                p["spec"] = copy.deepcopy(claude_sub)
                p["tool"] = "claude"
                p["name"] = f"{worker}-{slot}-{tag}x"
                p["file"] = f"{rd}/{tag}-{slot}2.md"
                p["prompt"] = _mk_prompt(p["file"])
                p["pane"] = (_spawn_reviewer(p["name"], base_pane, p["split"], cwd, p["spec"], p["prompt"])
                             or _spawn_reviewer(p["name"], base_pane, p["split"], cwd, p["spec"], p["prompt"]))
        # COLLECT helper: read a finished reviewer's verdict, re-prompt once if absent.
        # RUN — poll both reviewers CONCURRENTLY rather than waiting one out fully.
        # The reviewers run in parallel and EVERY slot runs to its own conclusion.
        #
        # This used to short-circuit: the first VERDICT: FAIL cancelled the other
        # reviewer, on the reasoning that the author must fix the failing review
        # regardless, so a second opinion only costs wall-clock. In practice that
        # silently reduced the panel to its fastest member. Measured 2026-10-07: a
        # grok reviewer at xhigh was spawned five times across two lanes and produced
        # ZERO verdicts, because codex reached FAIL first in every round and grok was
        # cancelled each time. The rounds where a FAIL happens are exactly the rounds
        # with the most to find, so the slower reviewer contributed nothing at all.
        # A second reviewer that never speaks is not a panel. Owner decision
        # (2026-10-07): collect both verdicts, and pay the wall-clock. The exposure
        # is bounded by timeout_mins, and a FAIL still reaches the author as soon as
        # the round ends.
        pending = []
        for slot, p in plan.items():
            if p["pane"]:
                _wait_status(p["name"], "working", 60_000)  # guard startup idle; ok to miss
                pending.append(slot)
            else:
                results[slot] = {"file": p["file"], "verdict": "SPAWN-FAILED", "tool": p["tool"]}
        deadline = time.time() + timeout_s
        nudge_at = time.time() + timeout_s // 2
        while pending and time.time() < deadline:
            for slot in list(pending):  # iterate a copy; we mutate pending below
                p = plan[slot]
                # THE FILE IS THE ONLY CONTRACT. Agent status is not a usable
                # terminal signal in EITHER direction, which cost four review rounds
                # across two lanes on 2026-09-25: herdr reports a codex agent "done"
                # while it is mid-turn (a reviewer 34s into reading files read as
                # done), and it can read "idle" between tool calls, so collecting on
                # a status flip harvests an unwritten file and reports MISSING on a
                # reviewer that is working normally. So: a written verdict decides the
                # slot immediately, and nothing else ends it early.
                v_file = _verdict_of(p["file"])
                if v_file in ("PASS", "FAIL"):
                    results[slot] = {"file": p["file"], "verdict": v_file, "tool": p["tool"]}
                    pending.remove(slot)
                    continue
                # Nothing written yet. Nudge ONCE at the half-way mark in case the
                # reviewer finished its analysis without writing the file, then keep
                # polling — an unwritten slot resolves as MISSING at the deadline, not
                # before it. A slow reviewer is never cut short.
                if not p.get("nudged") and time.time() >= nudge_at:
                    p["nudged"] = True
                    pane = _agent_pane(p["name"])
                    if pane:
                        _run("herdr", "pane", "send-text", pane,
                             f"Your review file {p['file']} is missing or lacks a final VERDICT line. Write it now, ending with VERDICT: PASS or VERDICT: FAIL.")
                        _run("herdr", "pane", "send-keys", pane, "Enter")
            time.sleep(3)  # pace the file poll; the verdict file is checked every pass
        # Anyone still pending hit the deadline — nothing is cancelled any more, so
        # CANCELLED is no longer a reachable verdict. Callers still read it
        # structurally for rounds recorded before 2026-10-07.
        for slot in pending:
            p = plan[slot]
            # Take whatever is on disk. No status wait, no second re-prompt — the
            # nudge already happened at the half-way mark.
            results[slot] = {"file": p["file"],
                             "verdict": _verdict_of(p["file"]) or "MISSING",
                             "tool": p["tool"]}
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
        f"{'issue':<9}{'state':<6}{'phase':<22}{'in-phase':<10}{'total':<9}+/-",
    ]
    for r in rows:
        icon = row_icon(r)
        st = state.get(str(r["issue"]), {})
        end = st.get("done", now)
        total = fmt_dur(end - st["start"]) if "start" in st else "-"
        in_phase = "-" if "done" in st or "phase_start" not in st else fmt_dur(now - st["phase_start"])
        d = r.get("diff")
        pm = f"+{d['add']}/-{d['del']}" if d else "-"
        out.append(f"#{str(r['issue']):<8}{icon:<5}{r['phase']:<22}{in_phase:<10}{total:<9}{pm}")
    for i, n in enumerate(queued):
        label = "queued ◀ next" if i == 0 else "queued"
        out.append(f"#{str(n):<8}{'⏳':<5}{label:<22}{'-':<10}{'-':<9}-")
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
    """One full monitoring tick — shared by legacy `watch` and the dashboard's
    collector process: discover workers, snapshot (phases, PR, rounds, tails),
    update timings + activity events, sweep finished reviewer panes, and apply
    workspace/agent/tab labels. Returns (rows, queued). Lifecycle (recycle/
    dispatch) is NOT here — the caller runs it after publishing the snapshot,
    so a slow dispatch never delays the dashboard's data."""
    ag = agents()
    active = list(issues_pin) or discover_issues(ag)
    q = list(queued_pin) or read_queue(active)
    rows = snapshot(active)
    update_timing(state, rows, record=True)
    sweep_reviewers(state, ag)
    save_state(state)
    apply_labels(rows, labels)
    return rows, q


# ---- lifecycle: monitor-owned recycle + dispatch -----------------------------
# Previously the dispatcher LLM's event loop recycled merged workspaces and
# dispatched queued issues — which stalled whenever that session was paused at a
# usage limit, compacted, or closed. These transitions are deterministic and
# ground-truth-driven, so the monitor owns them now (config [lifecycle]).

def root_path():
    return os.path.join(base(), "root.txt")


def repo_root():
    try:
        with open(root_path()) as f:
            return f.read().strip() or None
    except OSError:
        return None


def set_root(path=None):
    """Record the PRIMARY checkout's path — auto-dispatch needs it to cut
    worktrees and delete merged branches from any pane.

    Never records a LINKED worktree. `register()` refreshes this, and a worker
    registers from inside its own lane worktree — so without the guard the
    dispatcher's root becomes a lane checkout, every later dispatch tries to cut
    a worktree from inside another worktree, and the queue entries hit
    `_dispatch_fail: 3` and are silently dropped. Seen twice in one run, with
    nothing in the activity feed naming the cause.

    In a linked worktree `--git-common-dir` resolves to the primary repo's
    `.git`, so the primary checkout is its parent. An explicit `path` argument
    is honoured verbatim — a deliberate dispatcher call outranks the heuristic."""
    if path:
        p = path
    else:
        p = sh("git", "rev-parse", "--show-toplevel").strip() or None
        if p:
            common = sh("git", "rev-parse", "--path-format=absolute",
                        "--git-common-dir").strip()
            if common and os.path.basename(common) == ".git":
                primary = os.path.dirname(common)
                if primary and os.path.realpath(primary) != os.path.realpath(p):
                    p = primary  # we were in a linked worktree; use its primary
    if p:
        os.makedirs(base(), exist_ok=True)
        with open(root_path(), "w") as f:
            f.write(p)
    return p


def launch_author(pane, prompt_file, cwd):
    """Run the configured author agent in an existing pane via a launch script
    (prompt read from file at exec time — immune to typing truncation)."""
    os.makedirs(base(), exist_ok=True)
    script = os.path.join(base(), f"launch-worker-{os.getpid()}-{int(time.time())}.sh")
    quoted = " ".join(shlex.quote(a) for a in author_argv())
    # `herdr pane run` starts the worker from a fresh pane shell, so nothing in
    # this process's environment reaches it. A second run against the same repo
    # needs DUAL_AUTHOR_NS (its own registry/briefs/reviews) and usually its own
    # DUAL_AUTHOR_CONFIG (e.g. a different base_branch); without these the
    # worker re-derives the default namespace and reads the primary checkout's
    # .dual-author.toml, silently joining the OTHER run. Carry them explicitly.
    exports = "".join(f"export {k}={shlex.quote(os.environ[k])}\n"
                      for k in ("DUAL_AUTHOR_NS", "DUAL_AUTHOR_CONFIG")
                      if os.environ.get(k))
    with open(script, "w") as f:
        f.write(f'#!/bin/zsh\n{exports}cd {shlex.quote(cwd)}\n'
                f'exec {quoted} "$(cat {shlex.quote(prompt_file)})"\n')
    os.chmod(script, 0o755)
    sh("herdr", "pane", "run", pane, script)


def _pretrust(path):
    """Pre-accept trust dialogs for both agents so a fresh worktree doesn't
    stall the worker at a prompt."""
    try:
        p = os.path.expanduser("~/.claude.json")
        with open(p) as f:
            d = json.load(f)
        d.setdefault("projects", {}).setdefault(path, {})["hasTrustDialogAccepted"] = True
        with open(p, "w") as f:
            json.dump(d, f, indent=2)
    except Exception:
        pass
    try:
        cfgp = os.path.expanduser("~/.codex/config.toml")
        marker = f'[projects."{path}"]'
        existing = ""
        if os.path.exists(cfgp):
            with open(cfgp) as f:
                existing = f.read()
        if marker not in existing:
            with open(cfgp, "a") as f:
                f.write(f'\n[projects."{path}"]\ntrust_level = "trusted"\n')
    except Exception:
        pass


def _mark_in_progress(issue):
    """Label the issue in-progress and move its project-board items to
    Status = In Progress (any board it sits on; zero boards is fine)."""
    repo = _repo()
    if not repo:
        return
    sh("gh", "label", "create", "in-progress", "--repo", repo, "--color", "FBCA04",
       "--description", "dual-author agent working on it")
    sh("gh", "issue", "edit", issue, "--repo", repo, "--add-label", "in-progress")
    owner, name = repo.split("/", 1)
    query = ('query($owner:String!,$repo:String!,$num:Int!){'
             'repository(owner:$owner,name:$repo){issue(number:$num){'
             'projectItems(first:20){nodes{id project{id field(name:"Status")'
             '{... on ProjectV2SingleSelectField {id options{id name}}}}}}}}}')
    out = sh("gh", "api", "graphql", "-f", f"owner={owner}", "-f", f"repo={name}",
             "-F", f"num={issue}", "-f", f"query={query}")
    try:
        nodes = json.loads(out)["data"]["repository"]["issue"]["projectItems"]["nodes"]
    except Exception:
        return
    for it in nodes or []:
        proj = it.get("project") or {}
        fld = proj.get("field") or {}
        opt = next((o.get("id") for o in (fld.get("options") or [])
                    if re.search("in progress", o.get("name") or "", re.I)), None)
        if opt and it.get("id") and proj.get("id") and fld.get("id"):
            sh("gh", "project", "item-edit", "--id", it["id"], "--project-id",
               proj["id"], "--field-id", fld["id"], "--single-select-option-id", opt)


def _pop_queue(issue):
    try:
        with open(queue_path()) as f:
            q = [ln.strip() for ln in f if ln.strip()]
    except OSError:
        return
    q = [n for n in q if n.lstrip("#") != str(issue)]
    tmp = f"{queue_path()}.{os.getpid()}.tmp"
    try:
        with open(tmp, "w") as f:
            f.write("".join(f"{n}\n" for n in q))
        os.replace(tmp, queue_path())
    except OSError:
        pass


def _recycle(issue, state):
    """PR merged (gh ground truth) → the workspace has served its purpose:
    close leftover reviewer panes, unregister, remove workspace + worktree,
    delete the local branch (remote was deleted by --delete-branch)."""
    reg = load_registry()
    e = reg.get(str(issue)) or {}
    ws = e.get("ws")
    ag = agents()
    wa = _worker_agent(ag, issue, reg)
    wp = wa.get("pane_id") if wa else None
    for a in ag.values():
        if ws and a.get("workspace_id") == ws and a.get("pane_id") != wp:
            sh("herdr", "pane", "close", a["pane_id"])
    lane_branch = branch_for(issue)  # read BEFORE unregister drops the record
    _close_off_default(issue, state)
    requeued = _requeue_if_still_open(issue, state)
    unregister(issue)
    if ws:
        sh("herdr", "worktree", "remove", "--workspace", ws, "--force")
    root = repo_root()
    if root:
        sh("git", "-C", root, "branch", "-D", lane_branch)
    _push_event(state, issue,
                "♻ recycled (PR merged, requeued — issue still open)" if requeued
                else "♻ recycled (PR merged)", time.time())


REQUEUE_GRACE_SECS = 300  # GitHub's auto-close lands ~50s after a merge; wait well
                          # past that before deciding an issue was left open on purpose


def _drain_requeue_pending(state):
    """Decide the parked requeues from `_requeue_if_still_open` on settled state.

    An entry is parked when the merged PR carried a closing keyword, which means the
    issue MIGHT close by itself. After REQUEUE_GRACE_SECS, whatever GitHub did is what
    it meant: still open -> the issue really does have work left, so requeue it;
    closed -> drop it. Either way the entry leaves the pending map, so nothing
    accumulates and no issue is examined twice.

    An issue that has since been dispatched again is dropped without requeuing —
    otherwise it would sit in queue.txt while its own lane runs."""
    pending = state.get("_requeue_pending") or {}
    if not pending:
        return
    repo = _repo()
    now = time.time()
    active = set(load_registry())
    for n, parked in list(pending.items()):
        if now - parked < REQUEUE_GRACE_SECS:
            continue
        if n in active:
            pending.pop(n, None)
            continue
        ok, st = _gh("issue", "view", n, "--repo", repo, "--json", "state", "-q", ".state")
        if not ok:
            continue  # leave parked and retry next tick rather than guess
        pending.pop(n, None)
        if st.strip().lower() != "open":
            continue
        try:
            with open(queue_path()) as f:
                q = [ln.strip().lstrip("#") for ln in f if ln.strip()]
        except OSError:
            q = []
        if n in q:
            continue
        try:
            with open(queue_path(), "a") as f:
                f.write(n + "\n")
        except OSError:
            continue
        _PR_CACHE.pop(n, None)
        _push_event(state, n, "↩ requeued (PR merged, issue left open)", now)


def _merged_pr_autocloses(issue):
    """True when this lane's merged PR will close the issue BY ITSELF, shortly.

    GitHub honours a closing keyword ("Closes #N", "fixes #N", ...) in a PR body only
    on a merge into the DEFAULT branch, and it acts ASYNCHRONOUSLY — measured at ~50s
    after the merge. `_requeue_if_still_open` reads issue state the instant recycle
    fires, so it sees OPEN and requeues an issue that is about to close. The next tick
    then cuts a phantom lane with no work in it: because that lane never opens a PR,
    `pr_merged` never becomes true for it, recycle never fires, and its workspace sits
    in a slot until someone removes it by hand.

    Observed 2026-09-30: PR #1913 merged 13:06:08Z with "Closes #1818"; the requeue
    check saw OPEN, `issue/1818-r2` was cut, and the issue closed seconds later. Same
    sequence produced a phantom `issue/1855-r2`.

    Reading the keyword is better than sleeping on a grace delay: it is synchronous,
    and it distinguishes a PHASED issue ("Part of #N", which GitHub leaves open on
    purpose and which genuinely needs requeuing) from a finishing one."""
    info = pr_info(issue) or {}
    num = info.get("number")
    root = repo_root()
    if not num or not root:
        return False
    # Off the default branch GitHub ignores the keyword entirely; _close_off_default
    # owns that case, so a keyword there must NOT suppress the requeue.
    if (info.get("base") or "") != default_branch(root):
        return False
    ok, body = _gh("pr", "view", str(num), "--repo", _repo(), "--json", "body",
                   "-q", ".body")
    if not ok or not body:
        return False
    kw = r"(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)"
    n = re.escape(str(issue))
    return bool(re.search(rf"\b{kw}\b\s*:?\s+(?:#|\S+#|\S+/issues/){n}\b",
                          body, re.I))


def _lane_has_nothing_left(issue):
    """True when a lane has nothing left in flight: its issue is CLOSED and no PR of its
    own is still OPEN (none at all, or one already merged/closed).

    `recycle` keys on pr_merged, which is the right ground truth for the normal path but
    misses a lane that correctly concluded there was no code to write. #2248 investigated
    its failure, found it already fixed on main by #1919, closed the issue with that
    evidence and never opened a PR — so pr_merged stayed false forever and the lane sat
    on a slot for over two hours with the dashboard reporting phase "done".

    The open-PR half of the test is load-bearing, NOT a formality: an issue closed by the
    owner while its lane still has a PR in flight must be left alone to land that work.
    That happened twice on 2026-09-30 (#1770 and #1803, both closed by hand mid-flight
    with draft PRs worth merging), and tearing those lanes down would have thrown the
    work away."""
    repo = _repo()
    if not repo:
        return False
    ok, st = _gh("issue", "view", str(issue), "--repo", repo, "--json", "state", "-q", ".state")
    if not ok or st.strip().lower() == "open":
        return False
    info = pr_info(issue) or {}
    return (info.get("state") or "").upper() not in ("OPEN",)


def _requeue_if_still_open(issue, state):
    """A merged PR does not mean the issue is finished.

    A PHASED issue ships several PRs ("Part of #N", not "Closes #N"): each one
    merges into the default branch, GitHub leaves the issue open on purpose, and
    the next phase is still owed. Recycling on pr_merged alone therefore tore the
    lane down and dropped the issue from queue.txt for good — it stayed open with
    unchecked acceptance criteria and went on holding every dependent through
    respect_dependencies, with nothing left in the queue to clear it. That is how
    #1765/#1796/#1798/#1808 came to gate 20 queued issues while the fleet idled at
    4 of 10 lanes with a 29-entry queue.

    So: recycle the workspace either way (its branch is merged and its worktree is
    spent), but put a still-open issue BACK on the queue so the next tick can cut a
    fresh lane for its next phase. Returns True when it was requeued.

    Fails safe: any unresolved `gh` state leaves the queue untouched, because
    requeuing an issue that is actually closed would loop the lane forever."""
    repo = _repo()
    if not repo:
        return False
    ok, st = _gh("issue", "view", str(issue), "--repo", repo, "--json", "state",
                 "-q", ".state")
    if not ok or st.strip().lower() != "open":
        return False
    n = str(issue)
    if _merged_pr_autocloses(issue):
        # A closing keyword is a REASON TO WAIT, never a decision. GitHub acts
        # asynchronously (~50s), so requeuing now would cut a phantom lane — but it
        # also sometimes does not act at all: PR #1899 merged into main at 13:51:26Z
        # with "Closes #1747." and GitHub logged only a `referenced` event, never a
        # `closed` one, leaving #1747 open with an unchecked box and out of the queue.
        # Suppressing on the keyword alone traded one leak for another. So park it and
        # let a later tick decide on the state GitHub actually settled on.
        state.setdefault("_requeue_pending", {})[n] = time.time()
        return False
    try:
        with open(queue_path()) as f:
            q = [ln.strip().lstrip("#") for ln in f if ln.strip()]
    except OSError:
        q = []
    if n in q:
        return False
    try:
        with open(queue_path(), "a") as f:
            f.write(n + "\n")
    except OSError:
        return False
    # _PR_CACHE holds a MERGED entry as TERMINAL and never re-polls it, so without
    # this the next lane for the same issue reads the old PR as its own, recycles on
    # its first tick and requeues again — one hot loop per tick. _free_lane_branch
    # gives the new lane a branch with no merged PR, but only a LIVE poll sees that.
    _PR_CACHE.pop(n, None)
    _PR_CACHE.pop(str(issue), None)
    return True


def _close_off_default(issue, state):
    """Close the issue when its PR merged into a NON-default branch.

    GitHub honours "Closes #N" only for merges into the default branch. A lane
    merged into dispatch.base_branch therefore leaves its issue open, and every
    dependent stays held by respect_dependencies forever — including dependents
    in other repos, which read this issue's state through native blocked_by."""
    info = pr_info(issue) or {}
    merged_into = info.get("base")
    root = repo_root()
    if not merged_into or not root or merged_into == default_branch(root):
        return
    repo = _repo()
    ok, st = _gh("issue", "view", str(issue), "--repo", repo, "--json", "state",
                 "-q", ".state")
    if not ok or st.strip().lower() != "open":
        return
    note = (f"Merged into `{merged_into}` via #{info.get('number')}. Closed by "
            f"dual-author: GitHub only auto-closes issues for merges into the "
            f"default branch.")
    ok, _ = _gh("issue", "close", str(issue), "--repo", repo, "--reason",
                "completed", "--comment", note)
    _push_event(state, issue, (f"✓ closed issue (merged into {merged_into})" if ok
                               else f"⚠ could not close issue after merge into "
                                    f"{merged_into} — close it by hand"), time.time())


def _free_lane_branch(repo, root, n):
    """A branch name this lane can actually finish on.

    `issue/<N>` is right almost always. It is WRONG when a previous lane for the same
    issue already merged a PR from it — a deliverable split across two PRs, or any
    re-dispatch after a partial landing. `pr_info`'s merge ground truth polls by head
    branch, so that stale MERGED PR makes the monitor declare the fresh lane finished
    and recycle it seconds after dispatch, on every retry. Observed on a real run: the
    lane died twice before the branch was changed by hand.

    A leftover local/remote ref is also disqualifying — `worktree create` cannot cut a
    branch that already exists, and that failure is silent from the queue's side.

    Falls back to plain `issue/<N>` if the checks cannot run (no repo, gh down): a
    naming convenience must never wedge dispatch."""
    stem = f"issue/{n}"
    try:
        for i in range(1, 12):
            cand = stem if i == 1 else f"{stem}-r{i}"
            if root and sh("git", "-C", root, "rev-parse", "--verify", "--quiet",
                           f"refs/heads/{cand}").strip():
                continue
            if root and sh("git", "-C", root, "rev-parse", "--verify", "--quiet",
                           f"refs/remotes/origin/{cand}").strip():
                continue
            if repo:
                out = sh("gh", "pr", "list", "--repo", repo, "--head", cand,
                         "--state", "merged", "--limit", "1", "--json", "number")
                try:
                    if json.loads(out):
                        continue
                except Exception:
                    pass
            return cand
    except Exception:
        pass
    return stem


_DEFAULT_BRANCH = None


def default_branch(root):
    """The repo's default branch. Dispatch used to hardcode 'main', which fails
    outright on repos still on 'master' — there is no origin/main to fetch or
    branch from. Resolved from the remote once, then cached for the process."""
    global _DEFAULT_BRANCH
    if _DEFAULT_BRANCH:
        return _DEFAULT_BRANCH
    ref = sh("git", "-C", root, "symbolic-ref", "--short",
             "refs/remotes/origin/HEAD").strip()
    if not ref:
        # origin/HEAD is frequently unset on a clone; ask the remote directly.
        for line in sh("git", "-C", root, "ls-remote", "--symref",
                       "origin", "HEAD").splitlines():
            if line.startswith("ref:"):
                ref = line.split()[1]
                break
    name = ref.rsplit("/", 1)[-1] if ref else ""
    if not name:
        for cand in ("main", "master"):
            if sh("git", "-C", root, "rev-parse", "--verify", "--quiet",
                  f"refs/remotes/origin/{cand}").strip():
                name = cand
                break
    _DEFAULT_BRANCH = name or "main"
    return _DEFAULT_BRANCH


def lane_base(root):
    """The branch lanes are cut from and PRs merge into: dispatch.base_branch when
    set (an integration branch developed apart from main), else the default branch.
    Off the default branch GitHub neither targets PRs there nor closes issues on
    merge by itself — see _dispatch_next (gh-merge-base) and _close_off_default."""
    return cfg_get("dispatch.base_branch") or default_branch(root)


def _next_dispatchable(state, q):
    """The first queue entry with no OPEN blocker (dispatch.respect_dependencies).
    A blocked entry is SKIPPED, never popped — it stays in queue.txt and becomes
    dispatchable the moment its blockers close, so a whole dependency chain can be
    queued up front. Returns None when every entry is held (or the tick's lookup
    budget ran out; the next tick resumes against a warm cache). Each hold is
    announced once per blocker set in the activity feed.
    Before the blocker check, and whatever respect_dependencies says, the skip
    guard (skip_reason: dispatch.skip_labels, a PR, a CLOSED issue) POPS an
    entry that must never dispatch, with one feed line."""
    held = state.setdefault("_dep_held", {})
    deps = cfg()["dispatch"].get("respect_dependencies", True)
    if not deps:
        held.clear()
    live = {e.lstrip("#") for e in q}
    for k in [k for k in held if k not in live]:
        held.pop(k, None)  # left the queue — don't grow the state file forever
    deadline = time.time() + DEP_GATE_TICK_BUDGET
    for entry in q:
        n = entry.lstrip("#")
        why = skip_reason(n, deadline, state)
        if why is None:
            return None  # out of lookup budget, or a blind fail-closed lookup
        if why:
            _pop_queue(n)
            held.pop(n, None)
            _push_event(state, n, f"skipped: {why}", time.time())
            continue
        if not deps:
            return n
        blk = open_blockers(n, deadline, state)
        if blk is None:
            return None  # out of lookup budget this tick; resume next tick
        if blk:
            sig = ",".join(blk)
            if held.get(n) != sig:
                held[n] = sig
                _push_event(state, n, "⏸ held: blocked by "
                            + " ".join(f"#{b}" for b in blk), time.time())
            continue
        held.pop(n, None)
        return n
    return None


def _dispatch_next(state):
    """Dispatch the first dispatchable queued issue (see _next_dispatchable):
    fresh default branch → worktree workspace → pre-trust → brief
    (dispatcher-written file, else generated from the issue) → launch author →
    register → in-progress label/board → startup nudges.
    Returns True if an issue was dispatched."""
    root = repo_root()
    q = read_queue(discover_issues(None))
    if not root or not q:
        return False
    n = _next_dispatchable(state, q)
    if n is None:  # every queued entry is blocked / undetermined this tick
        return False
    fails = state.setdefault("_dispatch_fail", {})
    repo = _repo()
    # NB: not `base` — that name is the namespace-dir helper, used later in this
    # same function.
    base_branch = lane_base(root)
    sh("git", "-C", root, "fetch", "origin", base_branch)
    if sh("git", "-C", root, "rev-parse", "--abbrev-ref", "HEAD").strip() == base_branch:
        sh("git", "-C", root, "merge", "--ff-only", f"origin/{base_branch}")
    else:
        sh("git", "-C", root, "branch", "-f", base_branch, f"origin/{base_branch}")
    # `branch -f` refuses a branch checked out in ANY other worktree (an agent's
    # scratch checkout of an integration branch is common), and --ff-only fails on
    # a diverged one; either way the local ref is stale and a lane cut from it
    # starts behind. Cut from the remote-tracking ref whenever they disagree.
    cut_from = base_branch
    remote_sha = sh("git", "-C", root, "rev-parse", "--verify", "--quiet",
                    f"refs/remotes/origin/{base_branch}").strip()
    if remote_sha and remote_sha != sh("git", "-C", root, "rev-parse", "--verify",
                                       "--quiet", f"refs/heads/{base_branch}").strip():
        cut_from = f"origin/{base_branch}"
    lane_branch = _free_lane_branch(repo, root, n)
    out = sh("herdr", "worktree", "create", "--cwd", root, "--branch", lane_branch,
             "--base", cut_from, "--label", f"issue-{n}", "--no-focus", "--json")
    try:
        res = json.loads(out)["result"]
        ws = res["workspace"]["workspace_id"]
        wt = res["worktree"]["path"]
        pane = res["root_pane"]["pane_id"]
    except Exception:
        fails[n] = fails.get(n, 0) + 1
        if fails[n] >= 3:  # persistent failure must not hot-loop the queue head
            _pop_queue(n)
            _push_event(state, n, "✗ auto-dispatch failed 3x — dropped from queue "
                                  "(dispatch manually)", time.time())
        else:
            _push_event(state, n, f"auto-dispatch attempt {fails[n]} failed "
                                  f"(worktree create)", time.time())
        return False
    fails.pop(n, None)
    _pretrust(wt)
    # `gh pr create` without --base targets the DEFAULT branch; gh-merge-base is the
    # per-branch default it reads first, so a worker that omits --base still opens
    # the PR against the right branch. `git branch -D` at recycle drops it again.
    sh("git", "-C", root, "config", f"branch.{lane_branch}.gh-merge-base", base_branch)
    brief = os.path.join(base(), f"issue-{n}-brief.txt")
    if not os.path.exists(brief):
        info = sh("gh", "issue", "view", n, "--repo", repo, "--json", "title,body")
        try:
            d = json.loads(info)
            text = f"{d.get('title', '')}\n\n{d.get('body', '')}"
        except Exception:
            text = (f"GitHub issue #{n} in {repo} — brief fetch failed; run "
                    f"`gh issue view {n}` yourself for the full context.")
        with open(brief, "w") as f:
            f.write(text)
    launch = os.path.join(base(), f"issue-{n}-launch.txt")
    with open(launch, "w") as f:
        f.write(f"Read ~/.claude/skills/dual-author/SKILL.md and follow the WORKER "
                f"role exactly. You are in a git worktree on branch {lane_branch} for "
                f"GitHub issue #{n} — wherever the skill says issue/<N>, it means "
                f"{lane_branch}. Read {brief} for the full issue brief. "
                f"Base branch: {base_branch}.\n")
    launch_author(pane, launch, wt)
    time.sleep(3)
    register(n, ws, pane, lane_branch)
    sh("herdr", "agent", "rename", pane, worker_display(n, "⚙️", "starting"))
    _pop_queue(n)
    _push_event(state, n, "⚙ auto-dispatched", time.time())
    _mark_in_progress(n)
    # a fresh worktree can stack the security notice + MCP picker dialogs;
    # spaced Enters clear both (harmless empty submits once the composer is up)
    for _ in range(3):
        time.sleep(6)
        sh("herdr", "pane", "send-keys", pane, "Enter")
    return True


def lifecycle(state, rows):
    """Monitor-owned lifecycle pass, run AFTER the snapshot is published.
    Serialized by a machine-local lock so two watchers can't double-dispatch."""
    lc = cfg()["lifecycle"]
    if not (lc.get("recycle") or lc.get("dispatch")):
        return
    os.makedirs(base(), exist_ok=True)
    lockf = open(os.path.join(base(), "lifecycle.lock"), "w")
    try:
        if fcntl is not None:
            try:
                fcntl.flock(lockf, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return  # another watcher owns lifecycle this tick
        if lc.get("recycle"):
            reg = load_registry()
            for r in rows:
                n = str(r["issue"])
                if n in reg and (pr_merged(n) or _lane_has_nothing_left(n)):
                    _recycle(n, state)  # gh ground truth only
        _drain_requeue_pending(state)
        if lc.get("dispatch"):
            cap = int(cfg()["dispatch"]["parallel"])
            if len(discover_issues(None)) < cap:
                _dispatch_next(state)  # one per tick keeps ticks bounded
        save_state(state)
    finally:
        if fcntl is not None:
            with contextlib.suppress(OSError):
                fcntl.flock(lockf, fcntl.LOCK_UN)
        lockf.close()


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
             "worker-pane", "close-reviewers", "config", "author-launch", "set-root",
             "base-branch")
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
        launch_author(opts["pane"], opts["prompt-file"], opts["cwd"])
        print(f"launched author ({cfg()['author']['tool']}) in {opts['pane']}")
        return

    if mode == "set-root":
        # set-root [path]: record the primary checkout path (defaults to the
        # current repo's toplevel). Auto-dispatch/recycle need it to cut
        # worktrees and delete merged branches from any pane. Dispatcher calls
        # this once at setup, from the repo.
        p = set_root(rest[0] if rest else None)
        if not p:
            print("set-root: not in a git repo and no path given", file=sys.stderr)
            sys.exit(1)
        print(p)
        return

    if mode == "ns":
        # Resolved namespace for this repo — SKILL.md uses it to build the
        # matching /tmp/dual-author/<ns> paths.
        print(ns())
        return

    if mode == "base-branch":
        # The branch lanes are cut from and PRs merge into (dispatch.base_branch,
        # else the repo default). Workers use it for --base and review diffs.
        root = repo_root() or sh("git", "rev-parse", "--show-toplevel").strip()
        print(lane_base(root))
        return

    if mode == "register":
        # register <N> --workspace <ws> --pane <root_pane>: record the worker's
        # stable handles so the dispatcher can resolve it after its display name
        # starts carrying icon+phase. Called once per issue at dispatch.
        issue, args = rest[0].lstrip("#"), rest[1:]
        opts = {"workspace": None, "pane": None, "branch": None}
        while args:
            opts[args[0].lstrip("-")] = args[1]
            args = args[2:]
        register(issue, opts["workspace"], opts["pane"], opts["branch"])
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
            try:
                lifecycle(state, rows)
            except Exception as e:
                print(f"[dual-author] lifecycle error: {e}", flush=True)
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
            try:
                lifecycle(state, rows)  # AFTER the snapshot — a slow dispatch
                # (worktree + launch + nudges) must not delay dashboard data
            except Exception as e:
                _push_event(state, "-", f"lifecycle error: {e}", time.time())
                save_state(state)
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
