import argparse
import logging
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import select

from app.config import settings
from app.database import SessionLocal
from app.job_service import claim_next_job, execute_job
from app.models import Worker, WorkerHeartbeat, utcnow


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("worker")


class WorkerRuntime:
    def __init__(self, name: str, concurrency: int):
        self.name = name
        self.concurrency = concurrency
        self.stop_event = threading.Event()
        self.active = 0
        self.lock = threading.Lock()
        self.worker_id: str | None = None

    def register(self):
        with SessionLocal() as db:
            worker = db.scalar(select(Worker).where(Worker.name == self.name))
            if worker is None:
                worker = Worker(name=self.name, hostname=socket.gethostname(), concurrency=self.concurrency)
                db.add(worker)
            worker.status = "online"
            worker.hostname = socket.gethostname()
            worker.concurrency = self.concurrency
            worker.started_at = utcnow()
            worker.last_heartbeat_at = utcnow()
            db.commit()
            db.refresh(worker)
            self.worker_id = worker.id
        logger.info("Registered %s (%s)", self.name, self.worker_id)

    def heartbeat_loop(self):
        while not self.stop_event.wait(settings.heartbeat_seconds):
            with SessionLocal() as db:
                worker = db.get(Worker, self.worker_id)
                if worker:
                    worker.status = "online"
                    worker.last_heartbeat_at = utcnow()
                    with self.lock:
                        active = self.active
                    db.add(WorkerHeartbeat(worker_id=worker.id, active_jobs=active))
                    db.commit()

    def run_job(self, job_id: str):
        with self.lock:
            self.active += 1
        try:
            with SessionLocal() as db:
                execute_job(db, job_id, self.worker_id)
        finally:
            with self.lock:
                self.active -= 1

    def run(self):
        self.register()
        heartbeat = threading.Thread(target=self.heartbeat_loop, name="heartbeat", daemon=True)
        heartbeat.start()
        executor = ThreadPoolExecutor(max_workers=self.concurrency, thread_name_prefix=self.name)
        try:
            while not self.stop_event.is_set():
                with self.lock:
                    capacity = self.concurrency - self.active
                if capacity > 0:
                    with SessionLocal() as db:
                        job = claim_next_job(db, self.worker_id)
                    if job:
                        logger.info("Claimed job %s (%s)", job.id, job.job_type)
                        executor.submit(self.run_job, job.id)
                        continue
                self.stop_event.wait(settings.worker_poll_seconds)
        except KeyboardInterrupt:
            logger.info("Shutdown requested; waiting for active jobs")
        finally:
            self.stop_event.set()
            executor.shutdown(wait=True)
            with SessionLocal() as db:
                worker = db.get(Worker, self.worker_id)
                if worker:
                    worker.status = "offline"
                    worker.last_heartbeat_at = utcnow()
                    db.commit()
            logger.info("Worker stopped gracefully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a distributed scheduler worker")
    parser.add_argument("--name", default=f"worker-{socket.gethostname()}")
    parser.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()
    WorkerRuntime(args.name, max(1, args.concurrency)).run()
