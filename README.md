# dual-author

A self-orchestrating **issue pipeline** that runs as a [Claude Code](https://claude.com/claude-code) skill inside [herdr](https://github.com/herdr).

Point it at one or more GitHub issues (or a project board, label, or milestone) and it will, for each issue, in parallel:

1. **Create** a dedicated git worktree workspace.
2. **Implement** the issue with a named Claude worker agent and push a **draft PR**.
3. **Dual-review** the change with fresh `codex` + `claude` reviewer agents in split panes, plus PR review bots (CodeRabbit, Copilot, CI).
4. **Fix → re-review** with fresh reviewer instances each round until the diff is clean (no round cap).
5. **Mark the PR ready** and arm auto-merge — it merges only when all checks pass.

A live dashboard pane runs a full-screen [Textual](https://textual.textualize.io/) TUI: a selectable issues table (stage, timings, PR, checks), a per-issue detail panel (review-round verdicts, live worker output), an activity feed, and a `g` graph view: the blocking DAG between the run's issues (from GitHub issue dependencies) plus every issue's pipeline chain at once. It also live-renames each issue's workspace, agent, and tab (`#<N> · <repo>`) so the whole herdr UI doubles as a status board. Falls back to a plain-text render without `uv`.

## Requirements

- [herdr](https://github.com/herdr) — the skill must run inside a herdr-managed pane (`HERDR_ENV=1`).
- [Claude Code](https://claude.com/claude-code) (`claude` on your `PATH`).
- [`codex`](https://github.com/openai/codex) — used as the second reviewer.
- [`gh`](https://cli.github.com/) — authenticated (`gh auth login`) with `repo`, `project`, and `workflow` scopes.
- `python3` — runs the monitoring/orchestration script.
- [`uv`](https://docs.astral.sh/uv/) (recommended) — provisions the TUI dashboard's env (python ≥3.10 + textual) on first use; without it the dashboard falls back to a plain-text render.

## Install

Skills live in `~/.claude/skills/<name>/`. Clone this repo directly into that location:

```bash
git clone https://github.com/Tudor0404/dual-author.git ~/.claude/skills/dual-author
```

Or, if you prefer to keep it elsewhere and symlink:

```bash
git clone https://github.com/Tudor0404/dual-author.git ~/src/dual-author
ln -s ~/src/dual-author ~/.claude/skills/dual-author
```

That's it — no build step. Claude Code discovers the skill from its `SKILL.md` frontmatter on the next session.

### Verify

Start a Claude Code session and confirm the skill is available:

```bash
claude
> /dual-author
```

You should see the dual-author skill invoke. (Remember it only runs inside a herdr pane — `HERDR_ENV=1`.)

### Update

```bash
git -C ~/.claude/skills/dual-author pull
```

## Usage

Run from inside a herdr pane:

```
/dual-author 12 34                       # specific issue numbers
/dual-author board "Sprint 3"            # every open issue in a project board
/dual-author --parallel 5 101 102 103    # override the default concurrency (3)
/dual-author                             # no args → pick from open issues interactively
```

The dispatcher confirms the resolved work list before spawning anything, so you always see the blast radius first.

## Layout

```
dual-author/
├── SKILL.md            # the skill definition (dispatcher + worker roles)
└── scripts/
    ├── monitor.py      # polling / renames / review state-machine / data collection
    └── dashboard.py    # Textual TUI dashboard (run by `monitor.py watch` via uv)
```

## License

MIT
