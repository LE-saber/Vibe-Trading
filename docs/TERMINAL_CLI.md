# Terminal Tournament Strategy CLI

The terminal strategy can be operated without starting FastAPI. Run commands
from the `research` directory with the repository Python environment:

```powershell
Push-Location research
..\.venv\Scripts\python.exe scripts\terminal-strategy.py status
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
