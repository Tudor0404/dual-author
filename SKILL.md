---
name: dual-author
description: "Self-orchestrating issue pipeline inside herdr. For each GitHub issue: herdr creates a worktree workspace, a named Claude worker agent implements the issue, pushes a draft PR, spawns named codex + claude reviewer agents in tab splits, monitors PR bot comments and checks, fixes findings, and re-reviews with fresh reviewer instances each round until clean — then marks the PR ready and arms auto-merge (merges only when all checks pass). The dispatcher pane shows a live full-screen TUI dashboard (issues table, PR/checks/review detail, pipeline-graph view) and final verdict summary. Requires HERDR_ENV=1, gh, codex, claude; uv recommended for the TUI (plain-text fallback without it). Use when asked to dual-author, swarm issues, work through a project board, or auto-implement-and-review issues in herdr."
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

## Configuration

All model/agent/concurrency/merge choices live in a TOML config — **do not hardcode
them in commands**. `scripts/monitor.py` reads it at runtime; precedence (later wins):

1. built-in defaults in `monitor.py`
2. `~/.claude/skills/dual-author/config.toml` — the shipped defaults (edit this)
3. `<repo>/.dual-author.toml` — per-repo override (committable)
4. `$DUAL_AUTHOR_CONFIG=/path.toml` — explicit override

Key sections (see `config.toml` for the annotated full set):

- `[author]` — **which agent implements each issue.** `tool = "claude"` (default) or
  `tool = "codex"`. So the "main authoring" can be codex by flipping one line. `model` /
  `effort` (claude) or `model` (codex) tune it; codex authors get a permissive sandbox
  so they can push/gh/run tools.
- `[[review.reviewers]]` — the review panel (any mix/count of codex + claude, each with
  its own model/effort). Default is codex + claude. A codex slot that can't spawn is
  auto-substituted with claude for that round.
- `[dispatch]` — `parallel`: issues in flight (default 3). `respect_dependencies`
  (default true): auto-dispatch skips a queued issue that still has an OPEN blocker and
  takes the next unblocked entry instead (see *Dependency-aware dispatch* in step 2).
- `[lifecycle]` — **monitor-owned workspace lifecycle** (both on by default):
  `recycle` = on PR merge (gh ground truth) the monitor closes the issue's panes,
  unregisters it, removes the worktree workspace, and deletes the local branch;
  `dispatch` = the monitor itself dispatches queued issues (worktree → author launch
  → register → in-progress label/board) whenever a slot is free. The dispatcher LLM
  no longer babysits these transitions — a paused/buried session can't stall the run.
- `[review] timeout_mins`, `[merge]` (enabled/auto/method/delete_branch), `[timeouts]`.

Read a value in a command with `monitor.py config <dotted.key>` (e.g.
`monitor.py config dispatch.parallel`); dump the whole resolved config with
`monitor.py config`. Never bake a model name or agent into a launch line — go through
the config so a user edit takes effect without touching the skill.

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

### 2. Brief + queue the issues (the monitor dispatches them)

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
python3 ~/.claude/skills/dual-author/scripts/monitor.py set-root   # record the primary checkout
```

Use `$BASE/...` for every temp path. `set-root` records the primary checkout's path —
the monitor's auto-dispatch/recycle cut worktrees and delete merged branches from
panes outside the repo, so this must be run once, from the repo, before anything is
queued. The worker's **display** name is `⚙️ $NS-issue-$N · <phase>` (set by the
dashboard), but you never route by it — workers get `register`ed and resolved later
via `monitor.py worker-pane $N` (see below). (To run two namespaces for the *same*
repo, export `DUAL_AUTHOR_NS` before launching and pass it to workers — not needed
for distinct repos.)

**Base branch.** Lanes are cut from, and PRs merge into, the repo's default branch
unless `[dispatch] base_branch` names another one (an integration branch developed
apart from `main`). Resolve it once; everything below says `$BASE_BRANCH`:

```bash
BASE_BRANCH=$(python3 ~/.claude/skills/dual-author/scripts/monitor.py base-branch)
```

Off the default branch GitHub does two things differently, and the monitor covers
both: `gh pr create` without `--base` targets the default branch (auto-dispatch sets
`branch.<lane>.gh-merge-base`, and workers pass `--base`), and "Closes #N" does not
close the issue on merge (recycle closes it with a comment, so `respect_dependencies`
releases its dependents, cross-repo ones included).

**Sync `$BASE_BRANCH` to `origin/$BASE_BRANCH` ONCE, before the dispatch loop.**
`herdr worktree create --base <ref>` branches off that **local** ref — it does NOT
fetch. If it is behind the remote, every worktree it cuts starts stale (drives the
stale-diff and migration-collision failure modes). Fast-forward it from the primary
worktree first (`--ff-only` so a diverged/dirty branch fails loudly instead of
creating a merge commit — resolve by hand if it does):

```bash
git -C "$(git rev-parse --show-toplevel)" fetch origin "$BASE_BRANCH"
git -C "$(git rev-parse --show-toplevel)" merge --ff-only "origin/$BASE_BRANCH" \
  || { echo "local $BASE_BRANCH diverged from origin — reconcile before dispatching"; exit 1; }
```

If `$BASE_BRANCH` isn't the currently checked-out branch in the primary worktree, fetch
still advances the remote-tracking ref; use `git branch -f "$BASE_BRANCH"
"origin/$BASE_BRANCH"` (only when it is not checked out anywhere) instead of the
`merge --ff-only` above. Auto-dispatch does this itself, and cuts from
`origin/$BASE_BRANCH` whenever the local ref could not be synced (e.g. another agent
has the branch checked out in its own worktree).

**Default flow (`lifecycle.dispatch = true`, the shipped default): you do NOT create
worktrees or launch workers yourself.** For each issue, write a brief, then queue ALL
issues and start the dashboard (step 3). The monitor dispatches up to
`dispatch.parallel` issues itself — worktree off fresh `$BASE_BRANCH`, pre-trust, author
launch, registration, in-progress label + board Status — and back-fills from the
queue as issues merge and recycle:

```bash
# one brief per issue — title + full body + any context worth passing the worker
printf '%s\n\n%s\n' "<title>" "<full issue body / context>" > "$BASE/issue-$N-brief.txt"
# ALL issues go in the queue, preferred order; the monitor pops each as it dispatches
# (and skips any entry whose blockers are still open — dependency order is safe here)
printf '%s\n' 851 852 853 854 855 > "$BASE/queue.txt"
```

If a brief file is missing the monitor generates one from the issue title/body via
`gh issue view` — your hand-written brief is richer (labels, linked context, your
read of ambiguities), so still write them when you have context to add. The
in-progress label and board Status="In Progress" moves happen automatically at each
dispatch. `--parallel <n>` in the run's args: write it to the per-repo override
(`<repo>/.dual-author.toml`, `[dispatch] parallel = n`) so the monitor honors it.

**Verify pickup**: within ~30s of the dashboard starting, the first
`dispatch.parallel` issues should appear as ⚙️ rows (the queue drains one per
monitor tick). If nothing dispatches, check `set-root` was run and the dashboard's
activity feed for `auto-dispatch failed` events (a queue head that fails 3x is
dropped with an event — dispatch that issue manually, see below) or `held: blocked
by #N` events (every queued issue is waiting on a prerequisite — see below).

**Dependency-aware dispatch** (`[dispatch] respect_dependencies`, default true):
before dispatching an entry the monitor checks whether the issue still has an OPEN
blocker, from two sources unioned — GitHub's native issue dependencies (`gh api
repos/{owner}/{repo}/issues/{N}/dependencies/blocked_by`, entries whose state is
`open`) and the body convention `blocked by #123` / `Blocked by: #123` /
`depends on #123` / `requires #123`. A blocked entry is **skipped, not popped**: it
stays in `queue.txt`, the next unblocked entry dispatches instead, and the held one
goes automatically on a later tick once its blockers close. So **queue the whole
dependency chain up front and let the monitor order it** — you do NOT hand-gate
`queue.txt` for dependencies, and an unattended run can no longer start a worker
against an unmerged prerequisite (which cuts the branch off a base that lacks what
it depends on). Each hold is announced once per blocker set in the activity feed as
`#859 held: blocked by #858`, and the `[g]` graph view shows the same DAG.
The check **fails open**: if `gh` is missing, unauthenticated, rate-limited, times
out or answers something unparseable, the issue dispatches anyway and the feed says
`dependency check unavailable (gh)` once — a monitoring convenience must never wedge
the pipeline. Results are cached ~60s per issue and only entries actually up for
dispatch are checked. Set `respect_dependencies = false` for the old strict FIFO.

<details>
<summary><b>Manual dispatch</b> — only when <code>lifecycle.dispatch = false</code>
(or a dropped issue needs hand-dispatching)</summary>

For each issue `N`, one command creates the worktree (at
`~/.herdr/worktrees/<repo>/<branch>`), a new workspace, and its root pane:

```bash
WT_JSON=$(herdr worktree create --cwd "$(git rev-parse --show-toplevel)" \
  --branch "issue/$N" --base "$BASE_BRANCH" --label "issue-$N" --no-focus --json)
git config "branch.issue/$N.gh-merge-base" "$BASE_BRANCH"  # gh pr create's default base
# If issue/$N already exists, or already carries a MERGED PR from an earlier lane
# (a split deliverable whose PR 1 landed, or any re-dispatch after a partial land),
# use a distinct name — `issue/$N-r2` — and pass it to `register --branch` below.
# Auto-dispatch picks this for you via `_free_lane_branch`; do the same by hand here.
# Reusing a branch whose PR merged makes the monitor read that old PR as this lane's
# merge ground truth and recycle the lane seconds after it starts, on every retry.
WS=$(echo "$WT_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"]["workspace"]["workspace_id"])')
WT_PATH=$(echo "$WT_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"]["worktree"]["path"])')
ROOT_PANE=$(echo "$WT_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["result"]["root_pane"]["pane_id"])')
```

Pre-trust the worktree path for both agents (claude: set
`hasTrustDialogAccepted` under `projects."$WT_PATH"` in `~/.claude.json`; codex:
append a trusted `[projects."$WT_PATH"]` block to `~/.codex/config.toml`).

Start the worker **in the workspace's existing root pane** (do NOT `agent start
--workspace` — that adds a second pane and leaves the root shell orphaned). Write the
issue brief AND the short launch instruction to FILES — `monitor.py author-launch`
runs the **configured** author agent (claude or codex, per `config.toml [author]`) in
the pane via a launch script, so the tool/model isn't hardcoded and the prompt can't be
truncated mid-typing by `pane run`:

```bash
BRIEF="$BASE/issue-$N-brief.txt"   # written in the default-flow step above
LAUNCH="$BASE/issue-$N-launch.txt"
printf '%s\n' "Read ~/.claude/skills/dual-author/SKILL.md and follow the WORKER role exactly. You are in a git worktree on branch issue/$N for GitHub issue #$N. Read $BRIEF for the full issue brief. Base branch: $BASE_BRANCH." > "$LAUNCH"
python3 ~/.claude/skills/dual-author/scripts/monitor.py author-launch --pane "$ROOT_PANE" --prompt-file "$LAUNCH" --cwd "$WT_PATH"
# Register against STABLE handles (terminal id + workspace); the agent name is
# display-only (the dashboard renames it each tick). Resolve the worker later with
# `monitor.py worker-pane $N`, never by name.
sleep 3
python3 ~/.claude/skills/dual-author/scripts/monitor.py register "$N" --workspace "$WS" --pane "$ROOT_PANE" --branch "issue/$N"
herdr agent rename "$ROOT_PANE" "⚙️ $NS-issue-$N · starting"   # retry once if detection lags
# A fresh worktree can stack TWO claude startup dialogs (security notice + MCP
# picker); spaced Enters clear both. Then VERIFY the agent reaches working/idle.
for _ in 1 2 3; do sleep 6; herdr pane send-keys "$ROOT_PANE" Enter 2>/dev/null; done
grep -vx "$N" "$BASE/queue.txt" > "$BASE/queue.txt.new" && mv "$BASE/queue.txt.new" "$BASE/queue.txt"
```

Keep the prompt shell-safe (no unescaped quotes). Then mark the issue in-progress —
`gh label create in-progress ... ; gh issue edit "$N" --add-label in-progress`, and
move every project board it sits on to Status = "In Progress" (one GraphQL query for
projectItems + Status field/option ids, then `gh project item-edit` per board — the
monitor's `_mark_in_progress` in monitor.py is the reference implementation).

</details>

(No cleanup step needed: the merge closes the issue via `Closes #N`, and board
automations move closed issues to Done. For PRs that end draft/unmerged, the label
correctly stays.)

**Queue file**: `$BASE/queue.txt` (one issue number per line, dispatch order) is the
single source of pending work. Write it once after resolving the work list; the
monitor pops entries as it dispatches, and leaves in place any entry still blocked by
an open issue (with `lifecycle.dispatch = false`, rewrite it yourself each time you
dispatch, as in the manual block above). Top it up at any time — appending to the file
is enough, dependency-gated entries are safe to add before their blockers merge.

### 3. Monitoring — shell script, NOT self-re-prompting

Do NOT poll by repeatedly running herdr commands yourself — that burns tokens and
floods the transcript. All polling lives in `~/.claude/skills/dual-author/scripts/monitor.py`.

**Live dashboard pane** (full-screen Textual TUI, zero LLM involvement): split a pane off
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
auto-discovers active workers (via the registry, scoped to this repo's namespace) and
reads the pending queue from `$BASE/queue.txt`: newly dispatched issues appear on
their own, merged/recycled ones drop off, queued issues show as ⏳ rows with the
next-up one marked `◀ next`. It also shows per-issue elapsed time (total + time in
current phase), persisted in `$BASE/monitor-state.json` so even a dashboard restart doesn't
reset the clocks. Your only duty is keeping `queue.txt` current (step 2).

In a TTY, watch runs a full-screen Textual dashboard (via `uv run`, which provisions
python+textual in a cached env on first use — no manual install): a selectable issues
table (status, phase, timings, PR, checks), a detail panel for the selected issue
(PR + checks, per-round reviewer verdicts, a live tail of the worker's pane), an
activity feed of phase transitions, and a `[g]` graph view showing the blocking DAG
across the run's issues (GitHub issue dependencies + "blocked by #N" body
conventions, queued issues included) above every issue's full pipeline chain
(implement → draft PR → review rounds → checks → merge).
Keys: ↑↓/jk select · Enter/f focus the worker's pane · g graph · o open PR · r poll
· q quit (the pipeline keeps running). Without uv, or with `--legacy`, or when stdout
isn't a TTY, it falls back to the plain-text render.

Watch mode also live-renames each issue's workspace label AND worker-agent name to
its stage (`⚙️ <ns>-issue-852 · review-round-1`) and the issue's TAB to
`#852 · <repo-name>` (tabs otherwise sit at their default number), so the sidebar,
agents page, and tab strip all double as a status board — don't rename those
workspaces/tabs yourself while it runs.

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
  Record it, add `verdict-N` to `$SEEN`. Then:
  - **merged** → the monitor already handled it (config `[lifecycle]`): reviewer panes
    closed, issue unregistered, worktree workspace removed, local branch deleted, and
    the next queued issue dispatched into the free slot. Nothing to run — just record
    the outcome for the final summary. (With `lifecycle.recycle = false`, do it
    manually: `monitor.py close-reviewers N`, `monitor.py unregister N`,
    `herdr worktree remove --workspace <ws_id> --force`,
    `git -C <repo> branch -D issue/N`.)
  - **draft / auto-merge armed** (something failed or checks still pending) → leave the
    workspace open for inspection (and registered, so it keeps showing on the
    dashboard). The monitor never recycles an unmerged issue. Sweep stray reviewer
    panes if any: `monitor.py close-reviewers N`.
  - With `lifecycle.dispatch = false`, dispatch the next queued issue yourself.
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

You own one issue, one worktree (your cwd), one workspace. Your base branch is the one
named in your launch prompt (`Base branch: <name>`) — usually `main`, but a run can
target an integration branch instead, and then `main` is the WRONG base for your PR and
your review diffs. Your issue number `<N>` was given in your launch prompt. (Your agent's
display name is set by the dashboard to `⚙️ <ns>-issue-<N> · <phase>` and changes as you
progress — it's cosmetic; you never address yourself by it.) Resolve your namespace,
temp base and base branch once (same repo → same `<ns>` the dispatcher used):

```bash
NS=$(python3 ~/.claude/skills/dual-author/scripts/monitor.py ns)
BASE="/tmp/dual-author/$NS"; mkdir -p "$BASE"
BASE_BRANCH=$(python3 ~/.claude/skills/dual-author/scripts/monitor.py base-branch)
# must equal the `Base branch:` in your launch prompt — if not, trust the launch prompt
```

Wherever this role says `<base>`, substitute that branch name literally.

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
gh pr create --draft --base "$BASE_BRANCH" --title "<issue title> (#<N>)" --body "Closes #<N>. Dual-authored: implementation + codex/claude review loop in progress." 
PR=$(gh pr view --json number -q .number)
```

The PR exists from the start so review bots (CodeRabbit, Copilot, CI annotators) start
working in parallel with your local reviewers. Record the push timestamp — you'll only
act on comments newer than your latest push.

**Tests gate every review round.** Before spawning reviewers, run the relevant unit and
integration tests — the suites covering the packages/modules your diff touches, not the
full suite (CI is the full-suite backstop via the auto-merge gate). If anything fails,
fix it and re-run before proceeding: a reviewer round spent on a diff that fails its own
tests is a wasted round, and the Report phase needs passing tests as acceptance-criteria
evidence anyway. Run them in a plain `herdr pane` terminal if long-running.

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
Review the diff of this branch against <base> (git diff origin/<base>...HEAD) for correctness
bugs, security issues, and missed requirements of issue #<N>: <title>. Be specific,
file:line per finding.
PROMPT

python3 ~/.claude/skills/dual-author/scripts/monitor.py review <N> r<k> \
  --prompt-file "$RD/r<k>-prompt.txt" --cwd "$(pwd)"   # --timeout-mins defaults from config
```

The reviewer panel (which tools, models, how many) comes from `config.toml`
`[[review.reviewers]]` — the runner spawns whatever is configured. Do not assume
exactly codex+claude when reading results; iterate the JSON's slots.

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

### 3. Fix and re-review with FRESH reviewers (loop until clean)

Triage the combined findings — local reviewers + PR bot comments + failing checks.
Fix real issues, skip false positives (note why; reply to the bot comment via
`gh api ... -f body='...'` only if the user asked for that). Commit AND push fixes
(the push triggers bot re-review on the PR).

Then re-review with **fresh reviewer instances, not the same sessions** — a reviewer
that already passed your code is anchored on its own findings and is the wrong gate
for NEW bugs your fixes introduced. Each round reviews the full current diff cold:

Re-run the relevant unit and integration tests first — fixes break tests as easily as
the original implementation did; don't spawn a round on a diff that fails its own tests.
Then run the round-k review with the SAME state-machine command as step 2 — a new tag
(`r<k>`), same prompt file content, full diff (`git diff origin/<base>...HEAD`), no mention of
previous rounds, no summary of what you fixed: they must find problems independently.
The runner handles spawn/wait/collect/close — there is no manual sweep step anymore.
Stop when both fresh reviewers pass or remaining findings are only false positives.
There is no round cap — keep looping until the diff is genuinely clean.

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

Then mark the PR ready and merge per `config.toml [merge]` — with `auto = true` it merges
only once ALL checks pass; never merge with failing or pending checks yourself. With
`auto = false` (set on repos that have no branch protection, so arming always fails) merge
directly on the strength of the review gate; see **When auto-merge cannot be armed** below.
If `merge.enabled = false`, mark ready and STOP (a human merges):

```bash
gh pr ready "$PR"
MONITOR=~/.claude/skills/dual-author/scripts/monitor.py
if [ "$(python3 $MONITOR config merge.enabled)" = "true" ]; then
  FLAGS="--$(python3 $MONITOR config merge.method)"
  [ "$(python3 $MONITOR config merge.auto)" = "true" ] && FLAGS="--auto $FLAGS"
  [ "$(python3 $MONITOR config merge.delete_branch)" = "true" ] && FLAGS="$FLAGS --delete-branch"
  gh pr merge "$PR" $FLAGS
fi
```

After arming, wait (bounded, ~15 min) for the merge to actually land:
`gh pr view "$PR" --json state,mergedAt` until `MERGED` — a merged verdict lets the
dispatcher recycle your workspace for the next queued issue. If checks are still
running at the deadline, report `auto-merge armed` instead and stop.

**When auto-merge cannot be armed.** `gh pr merge --auto` fails on a repo with no
branch protection: *"Auto-merge could not be armed because GitHub reports that
protected branch rules are not configured."* Do NOT then fall back to
`gh pr checks "$PR" --watch` — a repo without protection usually has no CI either, so
that watches a set of zero checks and tells you nothing. Check first:

```bash
gh pr checks "$PR" 2>&1 | head -3   # "no checks reported" = there is no CI here
```

- **Checks exist** → watch them, and merge only when every one is green. A failing
  check is a new finding: fix → push → re-review → re-check, looping until clean.
- **No checks reported** → there is nothing for auto-merge to gate on, and the
  dual-author gate IS the gate. Merge directly with
  `gh pr merge "$PR" --squash --delete-branch`, on exactly the same authority you would
  have armed auto-merge with: two fresh reviewer PASSes on the current full diff, plus
  your own in-session suite green and its real counts in the PR body. Say in your report
  that you merged directly because the repo reports no checks.

Set `merge.auto = false` in that repo's config so no lane wastes a round on the arming
attempt. Never treat "no checks reported" as permission to skip the review gate or the
in-session suite — with no CI those two ARE the only gate, so they get stricter, not
looser. If anything still fails at the end, leave the PR draft and unmerged.

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

- **codex reviewer auth serialization.** codex on ChatGPT-subscription auth shares one
  `~/.codex/auth.json` with a single-use (rotating) refresh token. Concurrent reviewer
  spawns — multiple workers in one run, or two dual-author runs at once — used to race
  that refresh; the losers 401 "refresh token has already been used" and exit at startup,
  so the slot reports `SPAWN-FAILED` (claude reviewers are immune — different credential).
  `_spawn_reviewer` now holds a machine-global lock (`/tmp/dual-author-codex-auth.lock`)
  around each codex startup so refreshes serialize and write back before the next codex
  starts; review execution stays parallel. The runner self-heals: if a codex reviewer
  still can't start after its retries, the round substitutes a fresh claude into the
  codex slot for THAT round only (decided dual-reviewer round, not an undecided
  SPAWN-FAILED slot), and codex is attempted from scratch next round — so it resumes
  automatically the moment codex recovers, with no manual cleanup. Optional manual
  skip: a `codex-down` sentinel file in the namespace base dir forces the claude
  substitution up front (skips the wasted codex spawn attempt while codex is known
  dead). That flag SELF-EXPIRES after 90 min — drop it to skip codex now, but it can't
  silently pin every round to dual-claude forever the way the old never-cleared sentinel
  did. `touch <base>/codex-down` to refresh the window; `rm` it to resume codex sooner.
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
