# Terminal Tournament Strategy CLI

The terminal strategy can be operated without starting FastAPI. Run commands
from the `research` directory with the repository Python environment:

```powershell
Push-Location research
..\.venv\Scripts\python.exe scripts\terminal-strategy.py status
..\.venv\Scripts\python.exe scripts\terminal-strategy.py sync-inputs RUN_ID
..\.venv\Scripts\python.exe scripts\terminal-strategy.py evaluate RUN_ID
..\.venv\Scripts\python.exe scripts\terminal-strategy.py decision RUN_ID
..\.venv\Scripts\python.exe scripts\terminal-strategy.py candidates RUN_ID --tier watch
..\.venv\Scripts\python.exe scripts\terminal-strategy.py history RUN_ID --limit 10
..\.venv\Scripts\python.exe scripts\terminal-strategy.py plan RUN_ID
Pop-Location
```

Use `--runs-root PATH` before or after the command when operating a specific
ResearchTasks root. Every command writes bounded JSON to stdout. Failures write
stable, redacted JSON to stderr and return a non-zero exit code.

`evaluate` needs committed S4/S5 and a verified realtime observation. `plan`
also needs a complete terminal evaluation, current realtime and execution
status observations, and complete imported Ninef account/fee evidence. `plan`
creates only a shadow portfolio attempt; it never approves, executes, submits,
cancels, or force-enables orders.

## CLI-only terminal sequence

For a current research run, use this order:

```powershell
# 1. An existing completed S5 run is required.
..\.venv\Scripts\python.exe scripts\terminal-strategy.py sync-inputs RUN_ID

# 2. Save a fresh read-only quote observation. Supply the same account export
#    used for the Ninef input when held instruments must be included.
..\.venv\Scripts\python.exe scripts\check-realtime.py --run-id RUN_ID --account-file account.json

# 3. Save current execution references (still read-only).
..\.venv\Scripts\python.exe scripts\check-execution-status.py --run-id RUN_ID --account-file account.json

# 4. Evaluate and inspect the persisted decision/candidate tiers.
..\.venv\Scripts\python.exe scripts\terminal-strategy.py evaluate RUN_ID
..\.venv\Scripts\python.exe scripts\terminal-strategy.py decision RUN_ID
..\.venv\Scripts\python.exe scripts\terminal-strategy.py candidates RUN_ID --tier watch
..\.venv\Scripts\python.exe scripts\terminal-strategy.py candidates RUN_ID --tier focus
..\.venv\Scripts\python.exe scripts\terminal-strategy.py candidates RUN_ID --tier terminal

# 5. Generate a shadow-only portfolio attempt after all server facts are ready.
..\.venv\Scripts\python.exe scripts\terminal-strategy.py plan RUN_ID
```

`check-realtime.py` and `check-execution-status.py` are existing read-only
observation commands. They do not submit orders. If the configured
`award_rank_cutoff` or `award_rule_source` is not confirmed, `evaluate` remains
auditable but returns `UNAVAILABLE`; it never infers an award line from a
leaderboard display. `plan` is permanently shadow-only.
