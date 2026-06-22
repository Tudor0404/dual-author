---
name: dual-author
description: "Self-orchestrating issue pipeline inside herdr. For each GitHub issue: herdr creates a worktree workspace, a named Claude worker agent implements the issue, pushes a draft PR, spawns named codex + claude reviewer agents in tab splits, monitors PR bot comments and checks, fixes findings, and re-reviews with fresh reviewer instances each round until clean — then marks the PR ready and arms auto-merge (merges only when all checks pass). The dispatcher pane shows a live progress dashboard and final verdict summary. Requires HERDR_ENV=1, gh, codex, claude. Use when asked to dual-author, swarm issues, work through a project board, or auto-implement-and-review issues in herdr."
---

# dual-author — implement + dual-review issues in herdr

This skill has **two roles**. Decide which one you are FIRST:

- **DISPATCHER** — you were invoked via `/dual-author` by the user. You create worktree
  workspaces, launch named worker agents, run the live dashboard, and summarize.
- **WORKER** — your launch prompt explicitly says "follow the WORKER role". You
  implement one issue in your worktree and orchestrate your own reviewer agents.

Guardrail (both roles): if `HERDR_ENV` is not `1`, say you are not running inside a
herdr-managed pane and stop.

Use `herdr agent ...` for anything that is an agent (workers, reviewers) — named
targets, state waits, reads by name. Use `herdr pane ...` only for plain terminals
(running tests, tailing logs).

---

## DISPATCHER role

**Anchor every dispatcher-owned pane to `$HERDR_PANE_ID`.** The dashboard (and any pane
you split for yourself) must land in the pane `/dual-author` was invoked in, NOT the
focused pane. herdr's "the focused pane is yours" rule is WRONG here: the user navigates
away while the pipeline runs, so the focused pane drifts to a worker/other workspace and
splits would land there. `$HERDR_PANE_ID` identifies your pane regardless of focus, and
it's inherited **env** — present in every Bash call you make (a captured shell var would
NOT survive across calls), so just reference `$HERDR_PANE_ID` directly. Never resolve
"your pane" via `pane list`/focus.

### 1. Resolve work items

Args can be any of:

- **Issue numbers**: `/dual-author 12 34`
- **A project board**: `/dual-author board <name-or-number>` or natural language like
  "everything in the Sprint 3 board" → `gh project list --owner <owner>` to find it,
  then `gh project item-list <number> --owner <owner> --format json`; take open issues,
  optionally filtered by a status column the user names (e.g. "Todo").
- **A label or milestone**: `gh issue list --label X` / `--milestone X`.
- **No args**: `gh issue list --state open --limit 20` and ask which to dispatch
  (AskUserQuestion, multiSelect).

Before spawning, show the resolved list (count + titles) and confirm — the user should
see the blast radius first.

Gather context for each: `gh issue view <N> --json title,body,labels`.

### 2. Per-issue worktree workspace + worker agent

**Resolve the namespace once, up front, then PIN it.** All state, the queue, brief/
review files, the worker registry, and worker display names are namespaced by repo so a
*second* dual-author run against a different repo can proceed concurrently (even in the
same herdr session) without colliding on overlapping issue numbers. The namespace is a
slug of `gh repo view` — which resolves from the **current pane's cwd**. That is fine
for workers (they live inside the worktree) but NOT for the dispatcher's dashboard:
herdr's origin pane (`$HERDR_PANE_ID`) and the dashboard pane split off it can sit in
`~` or anywhere, where `gh repo view` finds nothing and `ns()` silently falls back to
`default` — a different namespace from the one the workers registered under, so the
dashboard reads an empty registry and renames nothing. So resolve `NS` once here (your
cwd is the repo at this point) and pass `DUAL_AUTHOR_NS=$NS` to EVERY `monitor.py`
process you launch in another pane — the dashboard especially. Do not rely on cwd
agreeing across panes:

```bash
NS=$(python3 ~/.claude/skills/dual-author/scripts/monitor.py ns)   # slug of owner/repo
BASE="/tmp/dual-author/$NS"; mkdir -p "$BASE"
```

Use `$BASE/...` for every temp path. The worker's **display** name is
`⚙️ $NS-issue-$N · <phase>` (set by the dashboard), but you never route by it — you
`register` each worker and resolve it later via `monitor.py worker-pane $N` (see below).
(To run two namespaces for the *same* repo, export `DUAL_AUTHOR_NS` before launching and
pass it to workers — not needed for distinct repos.)

For each issue `N`, one command creates the worktree (at
`~/.herdr/worktrees/<repo>/<branch>`), a new workspace, and its root pane:

```bash
WT_JSON=$(herdr worktree create --cwd "$(git rev-parse --show-toplevel)" \
  --branch "issue/$N" --base main --label "issue-$N" --no-focus --json)
WS=$(echo "$WT_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"]["workspace"]["workspace_id"])')
WT_PATH=$(echo "$WT_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"]["worktree"]["path"])')
```

**Pre-trust the worktree path** for both agents so nobody stops at a trust dialog:

```bash
ROOT_PANE=$(echo "$WT_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"]["root_pane"]["pane_id"])')
python3 - "$WT_PATH" <<'EOF'
import json, sys, os
p = os.path.expanduser('~/.claude.json')
d = json.load(open(p))
d.setdefault('projects', {}).setdefault(sys.argv[1], {})['hasTrustDialogAccepted'] = True
json.dump(d, open(p, 'w'), indent=2)
EOF
grep -qF "[projects.\"$WT_PATH\"]" ~/.codex/config.toml 2>/dev/null \
  || printf '\n[projects."%s"]\ntrust_level = "trusted"\n' "$WT_PATH" >> ~/.codex/config.toml
```

Start the worker **in the workspace's existing root pane** (do NOT `agent start
--workspace` — that adds a second pane and leaves the root shell orphaned). Write the
issue brief to a FILE and pass a SHORT prompt that references it — long inline prompts
get silently truncated mid-typing by `pane run`:

```bash
BRIEF="$BASE/issue-$N-brief.txt"
printf '%s\n\n%s\n' "<title>" "<full issue body / context>" > "$BRIEF"
herdr pane run "$ROOT_PANE" "claude --model opus --effort high 'Read ~/.claude/skills/dual-author/SKILL.md and follow the WORKER role exactly. You are in a git worktree on branch issue/$N for GitHub issue #$N. Read $BRIEF for the full issue brief. Base branch: main.'"
# Register the worker against STABLE handles (terminal id + workspace), then give it an
# initial display name. Routing now goes through the registry, NOT the agent name — the
# dashboard renames the agent to "⚙️ $NS-issue-$N · <phase>" each tick, so the name is
# display-only. Resolve the worker later with `monitor.py worker-pane $N`, never by name.
sleep 3
python3 ~/.claude/skills/dual-author/scripts/monitor.py register "$N" --workspace "$WS" --pane "$ROOT_PANE"
herdr agent rename "$ROOT_PANE" "⚙️ $NS-issue-$N · starting"   # retry once if detection lags
# A fresh worktree can stack TWO claude startup dialogs (the security notice, then
# the "new MCP servers found" picker) and the worker sits silently at them looking
# alive but doing nothing. Send Enter 3x, spaced, to clear both regardless of
# order; harmless empty-submits once the composer is up.
for _ in 1 2 3; do sleep 6; herdr pane send-keys "$ROOT_PANE" Enter 2>/dev/null; done
# VERIFY the worker actually started (agent status reaches working/idle with the
# composer up) — a pane can look launched while stuck at a dialog or a dead shell.
```

Keep the prompt shell-safe (no unescaped quotes).

**Mark the issue in-progress immediately after dispatch** (every issue, including
queued ones when their turn comes) so the board reflects pickup in real time:

```bash
gh label create in-progress --color FBCA04 --description "dual-author agent working on it" 2>/dev/null || true
gh issue edit "$N" --add-label in-progress
```

If the work items came from a **project board**, also move the item's Status to
"In Progress" — resolve the field/option ids once up front, reuse for every dispatch:

```bash
gh project field-list <number> --owner <owner> --format json   # → Status field id + "In Progress" option id
gh project item-edit --id <item-id> --project-id <project-id> \
  --field-id <status-field-id> --single-select-option-id <in-progress-option-id>
```

(No cleanup step needed: the merge closes the issue via `Closes #N`, and board
automations move closed issues to Done. For PRs that end draft/unmerged, the label
correctly stays.)

**Concurrency**: default 3 issues in flight (≈9 agents); `--parallel <n>` in args
overrides. Spawn up to the cap, queue the rest; when a worker prints its verdict,
dispatch the next queued issue (create its worktree lazily, at dispatch time).

**Queue file**: the dashboard reads the pending queue live from
`$BASE/queue.txt` (one issue number per line, dispatch order). Write it
right after resolving the work list, and rewrite it every time you dispatch a queued
issue:

```bash
printf '%s\n' 854 855 > "$BASE/queue.txt"
# when dispatching 854:
grep -vx 854 "$BASE/queue.txt" > "$BASE/queue.txt.new" && mv "$BASE/queue.txt.new" "$BASE/queue.txt"
```

### 3. Monitoring — shell script, NOT self-re-prompting

Do NOT poll by repeatedly running herdr commands yourself — that burns tokens and
floods the transcript. All polling lives in `~/.claude/skills/dual-author/scripts/monitor.py`.

**Live dashboard pane** (pure shell loop, zero LLM involvement): split a small pane off
`$HERDR_PANE_ID` (your origin pane — NOT the focused pane) and run watch mode in it, so
the dashboard lands in the workspace where `/dual-author` was called:

```bash
DASH=$(herdr pane split "$HERDR_PANE_ID" --direction down --no-focus | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"]["pane"]["pane_id"])')
# Pin DUAL_AUTHOR_NS — the dashboard pane's cwd may not be the repo, and without
# this the watcher resolves the wrong namespace, reads an empty registry, and never
# renames workspaces/agents (the symptom: sidebar stuck at the create-time label).
herdr pane run "$DASH" "DUAL_AUTHOR_NS=$NS python3 ~/.claude/skills/dual-author/scripts/monitor.py watch"
```

Argless watch is **self-updating** — start it ONCE and never restart it. Each tick it
auto-discovers active workers (agents named `$NS-issue-<N>`, scoped to this repo's
namespace) and reads the pending queue from `$BASE/queue.txt`: newly dispatched issues
appear on their own, merged/recycled ones drop off, queued issues show as ⏳ rows with
the next-up one marked `◀ next`. It also shows per-issue elapsed time (total + time in
current phase), persisted in `$BASE/monitor-state.json` so even a dashboard restart doesn't
reset the clocks. Your only duty is keeping `queue.txt` current (step 2).

Watch mode also live-renames each issue's workspace label to its stage
(`issue-852 ⚙️ review-round-1`), so the sidebar doubles as a status board — don't
rename those workspaces yourself while it runs.

**Your event loop**: block on wait mode in a single Bash call (timeout 600000); it
exits ONLY when something needs you, printing `EVENT ...` lines:

```bash
python3 ~/.claude/skills/dual-author/scripts/monitor.py wait --seen "$SEEN" 851 852 853
```

- `EVENT verdict <N>` → fires on ANY completion signal: the verdict block in the pane,
  a `phase: done` marker, **or the PR-merge ground truth** (the monitor polls
  `gh pr list --head issue/N --state merged` every 60s — pane text alone is lossy:
  verdict blocks scroll out of the read window, sessions pause at usage limits, and
  TUI redraws eat lines). Read the verdict block (`herdr agent read "$(python3
  ~/.claude/skills/dual-author/scripts/monitor.py worker-pane N)" --source
  recent-unwrapped --lines 120` — the worker's agent name now carries icon+phase and
  isn't addressable, so resolve its pane id); if it already scrolled away, get the facts
  from `gh pr view` instead — a merged PR is a finished issue regardless of pane state.
  Record it, add `verdict-N` to `$SEEN`. Sweep any reviewer panes the worker left open
  (it should have closed them, but enforce it — closes every non-worker pane in the
  issue's workspace):

  ```bash
  python3 ~/.claude/skills/dual-author/scripts/monitor.py close-reviewers N
  ```

  Then:
  - **merged** → the tab has served its purpose: drop it from the registry then remove
    it so a new issue takes its place — `python3
    ~/.claude/skills/dual-author/scripts/monitor.py unregister N`, then `herdr worktree
    remove --workspace <ws_id> --force` (removes workspace + checkout), then `git -C
    <repo> branch -D issue/N 2>/dev/null` (remote branch was deleted by
    `--delete-branch`). The dashboard drops the row as soon as it's unregistered — no
    restart needed.
  - **draft / auto-merge armed** (something failed or checks still pending) → leave the
    workspace open for inspection (and registered, so it keeps showing on the dashboard).
  - Either way, dispatch the next queued issue if any.
- `EVENT needs-input <N>` → read the worker's `=== NEEDS INPUT ===` block and print the
  **TL;DR right here** plus `herdr agent focus "$(python3
  ~/.claude/skills/dual-author/scripts/monitor.py worker-pane N)"` to jump there. The user
  should be able to decide from your pane alone. No NEEDS INPUT block → likely a
  permission prompt; say so. Add `input-N` to `$SEEN` (re-add as unseen if it blocks
  again later by removing it once the worker resumes working).
- `EVENT missing <N>` → the agent vanished (crash/closed); report it, add `missing-N`.
- `EVENT all-done` → final summary (step 4).
- Bash-tool timeout with no event → just re-run the same wait command.

Workers escalate only for architectural / user-owned decisions; routine questions they
answer themselves, so needs-input events should be rare.

### 4. Final summary

When all workers have a verdict (or ~20 min pass with no phase change), print the final
table: issue, PR (link + merged/auto-merge armed/draft), rounds, codex verdict, claude
verdict, bot comments addressed, checks, review files path — icon cells (✅/🔴/⚠️).

Merged issues' workspaces were already recycled during the run. Leave NON-merged
workspaces open — the user inspects and can chat with those workers directly; offer
their cleanup commands (`herdr worktree remove --workspace <id>`), never auto-run them.

---

## WORKER role

You own one issue, one worktree (your cwd), one workspace. Base branch is `main`. Your
issue number `<N>` was given in your launch prompt. (Your agent's display name is set by
the dashboard to `⚙️ <ns>-issue-<N> · <phase>` and changes as you progress — it's
cosmetic; you never address yourself by it.) Resolve your namespace and temp base once
(same repo → same `<ns>` the dispatcher used):

```bash
NS=$(python3 ~/.claude/skills/dual-author/scripts/monitor.py ns)
BASE="/tmp/dual-author/$NS"; mkdir -p "$BASE"
```

Use `$BASE/...` for every temp path below. The review runner auto-namespaces its own
output dirs and reviewer agent names, so `monitor.py review <N> ...` needs no ns flag.

**Phase markers**: the dispatcher reads your pane to drive a live dashboard. At every
transition, `echo "[dual-author] phase: <phase> ::"` — the trailing ` ::` sentinel lets
the monitor parse the token exactly even when the TUI wraps adjacent text into it
(without it, `review-round-1` + a wrapped timestamp parses as `review-round-12026`).
Phases are SINGLE hyphenated tokens: `implementing`, `pushing-pr`, `review-round-<k>`,
`fixing-round-<k>`, `awaiting-bots`, `blocked:<hyphenated-reason>`, `done`.

### 0. Autonomy and escalation policy

**Default to autonomous.** Answer questions yourself whenever a reasonable engineer
could decide from the issue, the codebase, or convention: reviewer questions,
bot comments, naming, test structure, library choice when the repo already uses one,
error-handling style, scope judgment on small ambiguities (pick the interpretation the
issue text best supports and note it). Reply to reviewers in their panes, resolve or
answer bot comments — do not stop for these.

**Escalate to the user ONLY when the decision genuinely belongs to them:**
- architectural decisions (new dependency, schema/API contract change, new service or
  pattern that future code will follow)
- anything destructive or hard to reverse beyond your branch
- the issue is contradictory or so underspecified that interpretations diverge widely
- security/payment/auth behavior changes

**How to escalate**: `echo "[dual-author] phase: blocked: <5-word reason>"`, then print
an escalation block and use AskUserQuestion in your pane (the dispatcher will point the
user at your workspace). Format — quick read first, depth after:

```
=== NEEDS INPUT: issue #<N> ===
TL;DR (1 min): <what you're building, the decision point, the 2-3 options, your
recommendation and why — a user who hasn't looked at this in 20 minutes must be able
to answer from this alone>

Full context: <the longer story: relevant code, constraints found, what each option
implies downstream, what reviewers/bots said — for when the TL;DR isn't enough>
```

After the answer, echo the phase you return to and continue autonomously.

### 1. Implement, push, open a draft PR

Implement the issue. Commit on the `issue/<N>` branch with a descriptive message. Then:

```bash
git push -u origin "issue/<N>"
gh pr create --draft --title "<issue title> (#<N>)" --body "Closes #<N>. Dual-authored: implementation + codex/claude review loop in progress." 
PR=$(gh pr view --json number -q .number)
```

The PR exists from the start so review bots (CodeRabbit, Copilot, CI annotators) start
working in parallel with your local reviewers. Record the push timestamp — you'll only
act on comments newer than your latest push.

### 2. Run a dual-review round (state machine — do NOT hand-roll panes)

The entire reviewer lifecycle (spawn → verify registration → name → wait → collect
verdict → ALWAYS close panes) is owned by ONE deterministic command. You never call
`herdr agent start` / `agent wait` / `pane close` for reviewers yourself — that is
how panes get orphaned.

Write your review prompt to a file (it must NOT contain the "write your review to
…/VERDICT" instruction — the runner appends that itself):

```bash
RD="$BASE/issue-<N>"; mkdir -p "$RD"
cat > "$RD/r<k>-prompt.txt" <<'PROMPT'
Review the diff of this branch against main (git diff main...HEAD) for correctness
bugs, security issues, and missed requirements of issue #<N>: <title>. Be specific,
file:line per finding.
PROMPT

python3 ~/.claude/skills/dual-author/scripts/monitor.py review <N> r<k> \
  --prompt-file "$RD/r<k>-prompt.txt" --cwd "$(pwd)" --timeout-mins 15
```

The runner blocks for the whole round (run it with a generous Bash timeout) and
prints JSON: `{"codex": {"file": ..., "verdict": "PASS|FAIL|CANCELLED|MISSING|SPAWN-FAILED"},
"claude": {...}}`. Exit 0 = the round was decided (every slot is PASS, FAIL, or
CANCELLED). It spawns the reviewers split off YOUR pane (it runs in your pane, so it
anchors on `$HERDR_PANE_ID` — never the focused pane), passes the prompt as a single argv
element (immune to typing truncation), retries a failed spawn once, re-prompts once if a
reviewer idles without writing its VERDICT, and closes both panes in a `finally` — no
orphans even on crash/timeout.

**Fail-fast short-circuit.** The two reviewers run concurrently and the runner polls
both. The **first** reviewer to return `VERDICT: FAIL` ends the round immediately: the
other reviewer is **cancelled** (its pane closed) and reported as `CANCELLED`. You do
NOT wait for a second opinion on a round that already failed — read the failing
reviewer's review file and go straight to fixing (step 3). A `CANCELLED` slot is
expected and fine; it is not an error and needs no re-run. Both reviewers only run to
completion when neither fails.

Then read the FULL review(s) from the file path(s) in the JSON (the Read tool — not
pane scrollback): on a FAIL short-circuit, read the failing slot's file (the
`CANCELLED` slot has no usable verdict); otherwise read both. `MISSING`/`SPAWN-FAILED`
after the runner's own retries is a real failure: re-run the round once with a fresh tag
(`r<k>b`); if it fails again, escalate (step 0).

For PHASED issues, tag rounds `p<phase>-r<k>` (e.g. `p2-r6`) — the tag is the file
prefix and the agent-name suffix; any single hyphenated token works.

### 2b. Monitor PR comments (you are the monitor)

While reviewers run — and again after each push — check the PR for new review
comments, especially from bots:

```bash
gh api "repos/{owner}/{repo}/pulls/$PR/comments" --paginate \
  -q '.[] | select(.created_at > "<last push ISO timestamp>") | {user: .user.login, path, line, body}'
gh pr view "$PR" --json reviews \
  -q '.reviews[] | select(.submittedAt > "<last push ISO timestamp>") | {author: .author.login, state, body}'
gh pr checks "$PR" 2>/dev/null || true
```

Treat unresolved bot findings and failing checks exactly like local reviewer findings —
they go into the same triage. Check once after spawning reviewers, then while blocked
on `agent wait` cycles; after your final push, do one last sweep with a grace window
(~3 min — `sleep 60` between up to 3 checks) so slow bots get a chance to land.

### 3. Fix and re-review with FRESH reviewers (max 3 rounds)

Triage the combined findings — local reviewers + PR bot comments + failing checks.
Fix real issues, skip false positives (note why; reply to the bot comment via
`gh api ... -f body='...'` only if the user asked for that). Commit AND push fixes
(the push triggers bot re-review on the PR).

Then re-review with **fresh reviewer instances, not the same sessions** — a reviewer
that already passed your code is anchored on its own findings and is the wrong gate
for NEW bugs your fixes introduced. Each round reviews the full current diff cold:

Run the round-k review with the SAME state-machine command as step 2 — a new tag
(`r<k>`), same prompt file content, full diff (`git diff main...HEAD`), no mention of
previous rounds, no summary of what you fixed: they must find problems independently.
The runner handles spawn/wait/collect/close — there is no manual sweep step anymore.
Stop when both fresh reviewers pass, remaining findings are only false positives, or
you hit 3 rounds.

### 4. Report

Print a final block (the dispatcher greps for the first line — emit it exactly once,
only when fully done):

If both reviewers passed and bot findings are addressed:

**Definition of Done — sync the issue to reality first.** Tick each acceptance-criteria
checkbox in the issue body that the merged work genuinely satisfies (backed by a passing
test or a green CI check). Leave unticked anything deferred, or written-but-not-executed
(e.g. an e2e spec with no CI job yet) — and say so. Then post ONE comment mapping each
criterion to the test/check that proves it, flagging any caveats. The boxes must reflect
what is actually proven, not just "Closes #N" — a closed issue is the durable record.

```bash
gh issue view "$N" --json body --jq .body > "$BASE/issue-$N-body.md"
# Tick ONLY the genuinely-proven criteria — edit the file by hand; do NOT blanket-tick.
# Leave deferred/uncovered boxes unchecked and note them in the comment.
gh issue edit "$N" --body-file "$BASE/issue-$N-body.md"
# AC -> evidence table: one row per criterion (test name + CI job), with a Caveats section
# for anything deferred or not CI-executed.
printf '## Acceptance criteria — evidence\n\n| Criterion | Proving test | CI job |\n|---|---|---|\n%s\n\n**Caveats:** %s\n' \
  "<rows>" "<deferred / not-yet-CI-executed items, or 'none'>" > "$BASE/issue-$N-accomment.md"
gh issue comment "$N" --body-file "$BASE/issue-$N-accomment.md"
```

Then mark the PR ready and enable auto-merge so it merges only once ALL checks pass —
never merge with failing or pending checks yourself:

```bash
gh pr ready "$PR"
gh pr merge "$PR" --auto --squash --delete-branch
```

After arming, wait (bounded, ~15 min) for the merge to actually land:
`gh pr view "$PR" --json state,mergedAt` until `MERGED` — a merged verdict lets the
dispatcher recycle your workspace for the next queued issue. If checks are still
running at the deadline, report `auto-merge armed` instead and stop.

If the repo doesn't allow auto-merge (`gh pr merge --auto` fails), verify checks
directly instead: `gh pr checks "$PR" --watch` and merge with `gh pr merge "$PR"
--squash` only when every check is green; if checks fail, treat it as a new finding
(fix → push → re-check, within your round budget). If anything still fails at the end,
leave the PR draft and unmerged.

**Re-review invariant — no commit reaches the merge gate unreviewed.** ANY commit made
after the last reviewer pass (a checks-fail fix, work following an escalation answer,
a late bot finding) voids that pass: run another fresh-reviewer round (step 3) on the
new full diff and get fresh PASSes before readying/merging. If auto-merge is already armed when new work
becomes necessary, disarm it first (`gh pr merge --disable-auto`), fix, re-review,
re-arm.

```
=== ISSUE #<N> VERDICT ===
branch: issue/<N>
pr: #<PR> <url> (merged|auto-merge armed|draft)
rounds: <k>
codex: PASS|FAIL — <one line>
claude: PASS|FAIL — <one line>
bots: <n> comments, <addressed/skipped summary>; checks: PASS|FAIL
acceptance: <n>/<m> criteria ticked (deferred: <list or none>); mapping comment posted
reviews: /tmp/dual-author/<ns>/issue-<N>/ (full text, all rounds)
files: <changed file list>
notes: <skipped false positives, open questions>
```

**Before printing the verdict block**, verify no reviewer pane of yours outlived its
round (the review runner closes them; the dispatcher's watch reaps stragglers): close
every pane in your workspace except your own (`$HERDR_PANE_ID`) —
`python3 ~/.claude/skills/dual-author/scripts/monitor.py close-reviewers <N>` does
exactly this. Then print the block, `echo "[dual-author] phase: done"`, and stop.
Reviews live in the temp files; your own pane stays for the user.

---

## Notes

- Worker agent names are DISPLAY strings (`⚙️ <ns>-issue-<N> · <phase>`, set by the
  dashboard) — never address a worker by name. Route via the registry: `monitor.py
  register` at dispatch, `monitor.py worker-pane <N>` to resolve its live pane. Reviewers
  keep stable per-round spawn names (the runner owns them); they're swept structurally
  (any non-worker pane in the issue's workspace), so their names don't matter for routing.
- Worktrees live at `~/.herdr/worktrees/<repo>/issue-<N>`; `herdr worktree remove
  --workspace <id>` cleans up both workspace and checkout (dispatcher offers, never auto-runs).
- `herdr integration install claude` / `codex` improves state detection and session
  identity — suggest once if waits behave oddly, don't auto-install.
- herdr ids compact when things close — parse ids from command output, never reuse
  stale ones. Worker pane ids can change as reviewer splits open/close, so always
  re-resolve with `monitor.py worker-pane <N>` (registry → stable terminal id) rather
  than caching a pane id.
- Parallelism: all workers run concurrently; each workspace is independent.
- Concurrent repos: a second `/dual-author` run against a *different* repo is safe to
  run at the same time — even in the same herdr session. State, queue, brief/review
  files, the registry (`/tmp/dual-author/<ns>/`) and worker display names
  (`<ns>-issue-<N>`) are namespaced by repo, and each run gets its own dashboard pane
  (anchored to its `$HERDR_PANE_ID`) that only sees its own namespace. The
  namespace is auto-derived (`monitor.py ns`), so nothing has to be coordinated between
  the two runs. (Two namespaces for the *same* repo needs `DUAL_AUTHOR_NS` exported and
  passed to workers.)
