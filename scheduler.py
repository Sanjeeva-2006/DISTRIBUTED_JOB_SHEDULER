import logging
import signal
import threading
from datetime import timedelta

from app.config import settings
from app.database import SessionLocal
from app.job_service import materialize_recurring_jobs, recover_stale_jobs
from app.models import utcnow


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("scheduler")
stop_event = threading.Event()


def stop(*_):
    stop_event.set()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    logger.info("Scheduler started")
    while not stop_event.is_set():
        with SessionLocal() as db:
            created = materialize_recurring_jobs(db)
            recovered = recover_stale_jobs(db, utcnow() - timedelta(seconds=settings.stale_worker_seconds))
            if created or recovered:
                logger.info("Created %s recurring jobs; recovered %s stale jobs", created, recovered)
        stop_event.wait(2)
    logger.info("Scheduler stopped")
