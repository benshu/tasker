import unittest
import datetime
import time

from .. import worker
from .. import scheduler


class DummyWorker(worker.Worker):
    name = 'dummy_test_worker'


class SchedulerTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.worker = DummyWorker()
        self.worker.init_worker()

        self.scheduler = scheduler.Scheduler()
        self.queue_name = 'scheduler_test_queue'

    @classmethod
    def tearDownClass(self):
        self.scheduler.terminate()
        self.worker.purge_tasks()

    def test_run_now(self):
        self.worker.purge_tasks()
        self.scheduler.start()

        self.assertEqual(
            first=self.worker.number_of_enqueued_tasks(),
            second=0,
        )
        self.scheduler.run_now(
            task=self.worker,
            args=[],
            kwargs={},
        )
        time.sleep(0.5)
        self.assertEqual(
            first=self.worker.number_of_enqueued_tasks(),
            second=1,
        )

        self.scheduler.stop()
        self.scheduler.clear()

    def test_run_in(self):
        self.worker.purge_tasks()
        self.scheduler.start()

        self.assertEqual(
            first=self.worker.number_of_enqueued_tasks(),
            second=0,
        )
        self.scheduler.run_in(
            task=self.worker,
            args=[],
            kwargs={},
            time_delta=datetime.timedelta(
                seconds=1,
            ),
        )
        for i in range(8):
            self.assertEqual(
                first=self.worker.number_of_enqueued_tasks(),
                second=0,
            )
            time.sleep(0.1)

        time.sleep(0.3)
        self.assertEqual(
            first=self.worker.number_of_enqueued_tasks(),
            second=1,
        )

        self.scheduler.stop()
        self.scheduler.clear()

    def test_run_at(self):
        self.worker.purge_tasks()
        self.scheduler.start()

        self.assertEqual(
            first=self.worker.number_of_enqueued_tasks(),
            second=0,
        )
        self.scheduler.run_at(
            task=self.worker,
            args=[],
            kwargs={},
            date_to_run_at=datetime.datetime.utcnow() + datetime.timedelta(
                seconds=1,
            ),
        )
        for i in range(8):
            self.assertEqual(
                first=self.worker.number_of_enqueued_tasks(),
                second=0,
            )
            time.sleep(0.1)

        time.sleep(0.3)
        self.assertEqual(
            first=self.worker.number_of_enqueued_tasks(),
            second=1,
        )

        self.scheduler.stop()
        self.scheduler.clear()

    def test_run_every(self):
        self.worker.purge_tasks()
        self.scheduler.start()

        self.assertEqual(
            first=self.worker.number_of_enqueued_tasks(),
            second=0,
        )
        self.scheduler.run_every(
            task=self.worker,
            args=[],
            kwargs={},
            time_delta=datetime.timedelta(
                seconds=1,
            ),
        )

        for i in range(3):
            for j in range(8):
                self.assertEqual(
                    first=self.worker.number_of_enqueued_tasks(),
                    second=i,
                )
                time.sleep(0.1)

            time.sleep(0.3)
            self.assertEqual(
                first=self.worker.number_of_enqueued_tasks(),
                second=i + 1,
            )

        self.scheduler.stop()
        time.sleep(1.2)
        self.assertEqual(
            first=self.worker.number_of_enqueued_tasks(),
            second=i + 1,
        )

        self.scheduler.clear()
