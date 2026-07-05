# Orbit Job Scheduler

Orbit is a SQLite-only background job scheduler MVP built for an internship assignment. It focuses on clean backend structure, queue processing, retries, worker recovery, and a small monitoring dashboard.

## Features

- JWT-based user registration and login
- Projects and queues
- Immediate, delayed, and recurring jobs
- Concurrent local workers
- Retry policies with fixed, linear, and exponential backoff
- Dead letter queue and manual retry
- Worker heartbeats and stale-worker recovery
- FastAPI backend with a simple HTML, CSS, and JavaScript dashboard

## Architecture

- `app/main.py` contains the FastAPI routes
- `app/models.py` defines the SQLAlchemy schema
- `app/job_service.py` contains claiming, execution, retries, recurring scheduling, and stale-worker recovery
- `worker.py` runs worker processes
- `scheduler.py` processes recurring jobs and stale workers
- `app/static/` contains the dashboard

This project is intentionally designed for a single-machine SQLite demo. Docker and PostgreSQL support were removed to keep the submission small and realistic for the assignment.

## Run locally

On Windows PowerShell:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

If script execution is blocked:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

Start the app in three terminals:

Terminal 1:

```powershell
uvicorn app.main:app --reload
```

Terminal 2:

```powershell
python scheduler.py
```

Terminal 3:

```powershell
python worker.py --name worker-1 --concurrency 4
```

Open `http://localhost:8000`.

## Example job types

- `add_numbers`
- `sleep_job`
- `generate_report`
- `failing_job`

## Tests

```bash
pytest -q
```
