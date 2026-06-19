#!/usr/bin/env python3
"""dual-author monitor — pure-shell monitoring so the dispatcher LLM doesn't poll.

Usage:
  monitor.py watch [<issue>...]                              loop forever, redraw dashboard (run in a pane)
  monitor.py wait  [--seen ev1,ev2] [--queued N,N] <issue>... block until an unseen event, print it, exit 0
  monitor.py review <issue> <tag> --prompt-file F [--cwd D] [--timeout-mins M]
        run ONE full dual-review round as a state machine: spawn codex+claude
        reviewer panes off the worker pane, verify registration, name them
        issue-<issue>-{codex,claude}-<tag>, wait to idle, verify the review
        files end with a VERDICT line (one re-prompt if not), then ALWAYS
        close both panes (finally). Prints JSON {codex:{file,verdict},
        claude:{file,verdict}} and exits 0. Reviews land in
        /tmp/dual-author/issue-<issue>/<tag>-{codex,claude}.md.
        Prompts pass as a single argv element — immune to typing truncation.

watch with NO issues is the normal mode: every tick it auto-discovers active workers
(agents named issue-<N>) and reads the dispatch queue from /tmp/dual-author/queue.txt
(one issue number per line, dispatch order — maintained by the dispatcher). New issues
appear automatically; merged/recycled ones drop off; queued ones show as ⏳ rows with
the next one marked. No restarts needed. Explicit positional issues pin the active set
instead (legacy).

watch also auto-sweeps reviewer panes (issue-<N>-codex-*/claude-*): idle ≥ 3 min →
closed (review file is on disk; nobody reads the pane); unknown ≥ 10 min → reaped.
Workers must treat a vanished reviewer whose review file ends with a VERDICT line as
a completed round, not a failure.

--queued (wait mode, optional) lists issues to render as ⏳; wait still requires an
explicit active list so it can detect missing agents.
Per-issue timing (total elapsed + time in current phase) persists in
/tmp/dual-author/monitor-state.json so dashboard restarts don't reset clocks.

Events printed by `wait` (one per line, after a final dashboard render):
  EVENT verdict <N>       worker printed its === ISSUE #N VERDICT === block
  EVENT needs-input <N>   worker is blocked / printed === NEEDS INPUT ===
  EVENT missing <N>       agent issue-<N> disappeared (crashed or closed)
  EVENT all-done          every issue has a verdict

--seen takes handled event ids: verdict-<N>, input-<N>, missing-<N>.
"""
import json
import os
import re
import shlex
import subprocess
import sys
import time

STATE_PATH = "/tmp/dual-author/monitor-state.json"
QUEUE_PATH = "/tmp/dual-author/queue.txt"
ISSUE_AGENT_RE = re.compile(r"^issue-(\d+)$")
REVIEWER_AGENT_RE = re.compile(r"^issue-\d+-(codex|claude)-\S+$")
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
    """Active issues = agents named exactly issue-<N> (reviewers are issue-<N>-*-r<k>)."""
    found = [m.group(1) for m in (ISSUE_AGENT_RE.match(n) for n in ag) if m]
    return sorted(set(found), key=int)


def read_queue(active):
    try:
        with open(QUEUE_PATH) as f:
            q = [ln.strip().lstrip("#") for ln in f if ln.strip()]
    except OSError:
        return []
    return [n for n in q if n not in set(active)]


def snapshot(issues):
    ag = agents()
    rows = []
    for n in issues:
        a = ag.get(f"issue-{n}")
        if not a:
            # a vanished agent whose PR merged FINISHED — report verdict, not missing
            rows.append({"issue": n, "status": "missing", "phase": "-",
                         "verdict": pr_merged(n), "input": False})
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
        })
    return rows


def active_round_issues():
    """Issue numbers with a live `monitor.py review <N> ...` process — their reviewer
    panes are owned by that runner and must not be swept."""
    out = sh("ps", "-eo", "command")
    issues = set()
    for line in out.splitlines():
        m = re.search(r"monitor\.py review\s+#?(\d+)\b", line)
        if m:
            issues.add(m.group(1))
    return issues


def sweep_reviewers(state, ag):
    """Deterministically close finished reviewer panes (watch mode, every tick).

    A reviewer agent (issue-<N>-codex-r<k> / issue-<N>-claude-<anything>) that has
    been idle for REVIEWER_IDLE_SWEEP_SECS is done — its review file is on disk and
    the worker reads the FILE, never the pane. unknown-status reviewers (crashed)
    get a longer forensics window, then are reaped too. Workers still sweep their
    own panes per SKILL.md; this is the backstop that makes cleanup unconditional.
    """
    now = time.time()
    rv = state.setdefault("_reviewers", {})
    # NEVER sweep a reviewer whose round runner is still alive — the runner owns its
    # panes and closes them in finally. Sweeping under it makes the runner wait on a
    # dead pane to its timeout (the two cleanup systems racing). active_round_issues()
    # reads the live `monitor.py review <N>` processes.
    protected = active_round_issues()
    worker_ws = {a.get("workspace_id"): (n, a["pane_id"]) for n, a in ag.items() if ISSUE_AGENT_RE.match(n)}
    ws_issue = {ws: re.match(r"issue-(\d+)", n).group(1) for ws, (n, _) in worker_ws.items()}
    candidates = {}
    for name, a in ag.items():
        m = REVIEWER_AGENT_RE.match(name)
        if m:
            issue = re.match(r"issue-(\d+)", name).group(1)
            if issue in protected:
                continue
            candidates[name] = a  # named reviewer, no live runner
        elif a.get("workspace_id") in worker_ws and a["pane_id"] != worker_ws[a["workspace_id"]][1]:
            # UNNAMED agent sharing an issue workspace = reviewer whose rename was
            # skipped (the common orphan). Key by pane id instead of name.
            if ws_issue.get(a.get("workspace_id")) in protected:
                continue
            candidates[f"_pane:{a['pane_id']}"] = a
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
    (deterministic regardless of the user's session defaults); codex has no
    equivalent flags and launches bare."""
    if tool == "claude":
        return ["claude", "--model", "opus", "--effort", "high"]
    return [tool]


def _spawn_reviewer(name, base_pane, split, cwd, tool, prompt):
    """Spawn one reviewer; return its pane_id or None. Verifies a LIVE agent.

    Primary: `herdr agent start` (prompt as argv — no typing). Verified via the
    agent registry, never by parsing stdout shape. Fallback: split a pane and run
    a LAUNCH SCRIPT — the pane types only a short script path, so a socket hiccup
    cannot truncate the prompt (typing the long command directly is how rounds
    used to end up as dead half-typed shells).
    """
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
    os.makedirs("/tmp/dual-author", exist_ok=True)
    script = f"/tmp/dual-author/launch-{name}.sh"
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
    rd = f"/tmp/dual-author/issue-{issue}"
    os.makedirs(rd, exist_ok=True)
    base_pane = _agent_pane(f"issue-{issue}")
    if not base_pane:
        print(json.dumps({"error": f"no agent named issue-{issue} to anchor splits"}))
        return 1
    plan = {}
    # /tmp/dual-author/codex-down sentinel (dispatcher-managed): codex CLI is
    # quota-dead at OpenAI. Fill the codex SLOT with a second independent claude
    # instance so every round still gets two reviewers. The result key stays
    # "codex" (callers read it structurally); "tool" records the substitution.
    codex_down = os.path.exists("/tmp/dual-author/codex-down")
    for slot, split in (("codex", "right"), ("claude", "down")):
        tool, name, outfile = slot, f"issue-{issue}-{slot}-{tag}", f"{rd}/{tag}-{slot}.md"
        if slot == "codex" and codex_down:
            tool = "claude"
            name = f"issue-{issue}-claude-{tag}x"   # still matches the sweep regex
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
        for slot, p in plan.items():  # RUN
            if not p["pane"]:
                results[slot] = {"file": p["file"], "verdict": "SPAWN-FAILED", "tool": p["tool"]}
                continue
            _wait_status(p["name"], "working", 60_000)  # guard startup idle; ok to miss
            _wait_status(p["name"], "idle", timeout_s * 1000)
            v = _verdict_of(p["file"])
            if v is None:  # COLLECT: one re-prompt
                pane = _agent_pane(p["name"])
                if pane:
                    _run("herdr", "pane", "send-text", pane,
                         f"Your review file {p['file']} is missing or lacks a final VERDICT line. Write it now, ending with VERDICT: PASS or VERDICT: FAIL.")
                    _run("herdr", "pane", "send-keys", pane, "Enter")
                    _wait_status(p["name"], "working", 30_000)
                    _wait_status(p["name"], "idle", (timeout_s // 2) * 1000)
                    v = _verdict_of(p["file"])
            results[slot] = {"file": p["file"], "verdict": v or "MISSING", "tool": p["tool"]}
    finally:  # CLEAN — unconditional, name-independent (uses tracked pane ids)
        for p in plan.values():
            pane = _agent_pane(p["name"]) or p["pane"]
            if pane:
                _run("herdr", "pane", "close", pane)
    print(json.dumps(results))
    return 0 if all(r["verdict"] in ("PASS", "FAIL") for r in results.values()) else 1


def load_state():
    try:
        with open(STATE_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state):
    # pid-unique tmp: watch + wait (+ review) run concurrently; a shared tmp name
    # races on os.replace (FileNotFoundError when the other process wins).
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    tmp = f"{STATE_PATH}.{os.getpid()}.tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(state, f)
        os.replace(tmp, STATE_PATH)
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


def render(rows, state, queued=()):
    now = time.time()
    out = [
        f"dual-author — {time.strftime('%H:%M:%S')}",
        f"{'issue':<9}{'state':<6}{'phase':<22}{'in-phase':<10}total",
    ]
    for r in rows:
        icon = "✅" if r["verdict"] else ("🔴" if r["input"] or r["status"] == "blocked" else ICON.get(r["status"], "❔"))
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
    if len(sys.argv) < 2 or sys.argv[1] not in ("watch", "wait", "review"):
        print(__doc__)
        sys.exit(2)
    mode, rest = sys.argv[1], sys.argv[2:]

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
        labels = {}  # workspace_id -> last label we set, to avoid rename churn
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
                ws = r.get("workspace_id")
                if not ws:
                    continue
                icon = "✅" if r["verdict"] else ("🔴" if r["input"] or r["status"] == "blocked" else "⚙️")
                label = f"issue-{r['issue']} {icon} {r['phase']}"
                if labels.get(ws) != label:
                    sh("herdr", "workspace", "rename", ws, label)
                    labels[ws] = label
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
