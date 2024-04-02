import concurrent.futures
import time

from config import config
from sbosc.component import SBOSCComponent
from sbosc.const import Stage, WorkerStatus
from sbosc.worker import Worker


class WorkerManager(SBOSCComponent):
    def __init__(self):
        super().__init__()
        self.desired_thread_count = 0
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=config.MAX_THREAD_COUNT)
        self.worker_threads = []
        self.created_threads = 0

    @property
    def thread_count(self):
        return len(self.worker_threads)

    def set_stop_flag(self):
        self.logger.info("Stopping worker manager...")
        self.stop_flag = True

    def start(self):
        self.logger.info("Worker manager started")
        while not self.stop_flag:
            self.check_worker_status()
            self.calculate_metrics()

            self.desired_thread_count = self.redis_data.worker_config.thread_count or 0
            self.logger.info(
                f"Current thread count: {self.thread_count}, "
                f"desired thread count: {self.desired_thread_count}"
            )
            if self.thread_count < self.desired_thread_count:
                self.add_threads()
            elif self.desired_thread_count < self.thread_count:
                self.remove_threads()
            time.sleep(60)

        # Stop all workers
        self.logger.info("Stopping workers...")
        self.desired_thread_count = 0
        self.remove_threads()
        self.redis_data.worker_metric.delete()
        self.logger.info("All workers stopped")
        self.logger.info("Worker manager stopped")

    def add_threads(self):
        self.logger.info(f"Adding {self.desired_thread_count - self.thread_count} threads")
        while self.thread_count < self.desired_thread_count:
            self.created_threads += 1
            worker = Worker(f'worker_{self.created_threads}', self)
            self.worker_threads.append((worker, self.executor.submit(worker.start)))

    def remove_threads(self):
        self.logger.info(f"Removing {self.thread_count - self.desired_thread_count} threads")
        removed_threads = []
        while self.thread_count > self.desired_thread_count:
            worker, thread = self.worker_threads.pop()
            worker.set_stop_flag()
            removed_threads.append(thread)

        # Wait for threads to stop
        concurrent.futures.wait(removed_threads, timeout=120)

    def check_worker_status(self):
        self.worker_threads = [
            (worker, thread) for worker, thread in self.worker_threads if not thread.done()
        ]  # Remove finished threads. This will trigger add_threads() on the next loop.
        busy_worker_count = len([
            worker for worker, thread in self.worker_threads if worker.status == WorkerStatus.BUSY
        ])
        self.logger.info(f"Busy worker count: {busy_worker_count}")
        if busy_worker_count == 0 and self.redis_data.current_stage == Stage.BULK_IMPORT \
                and len(self.redis_data.chunk_stack) == 0:
            self.redis_data.set_current_stage(Stage.BULK_IMPORT_VALIDATION)

    def calculate_metrics(self):
        datapoints = []
        for worker, _ in self.worker_threads:
            datapoints += list(worker.datapoints)
        if datapoints:
            average_insert_rate = sum(datapoints) / len(datapoints)
            self.redis_data.worker_metric.average_insert_rate = average_insert_rate
