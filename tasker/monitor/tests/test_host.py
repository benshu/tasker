import unittest
import time
import datetime

from .. import host
from .. import worker


class HostTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def test_run_time(self):
        host_obj = host.Host(
            name='test_host_name',
        )
        worker_obj_one = worker.Worker(
            name='test_worker_one_name',
        )

        self.assertEqual(
            first=host_obj.run_time,
            second=0,
        )

        host_obj.workers.append(worker_obj_one)

        time.sleep(0.5)

        self.assertLess(
            a=abs(host_obj.run_time - worker_obj_one.run_time),
            b=0.1,
        )

    def test_last_seen(self):
        host_obj = host.Host(
            name='test_host_name',
        )
        worker_obj_one = worker.Worker(
            name='test_worker_one_name',
        )
        worker_obj_one.last_seen = datetime.datetime.utcfromtimestamp(1000)
        worker_obj_two = worker.Worker(
            name='test_worker_two_name',
        )
        worker_obj_two.last_seen = datetime.datetime.utcfromtimestamp(2000)

        self.assertEqual(
            first=host_obj.last_seen,
            second=datetime.datetime.utcfromtimestamp(0),
        )

        host_obj.workers.append(worker_obj_one)

        self.assertEqual(
            first=host_obj.last_seen,
            second=datetime.datetime.utcfromtimestamp(1000),
        )

        host_obj.workers.append(worker_obj_two)

        self.assertEqual(
            first=host_obj.last_seen,
            second=datetime.datetime.utcfromtimestamp(2000),
        )

    def test_reports(self):
        for report_type in (
            'success',
            'failure',
            'retry',
        ):
            host_obj = host.Host(
                name='test_host_name',
            )
            worker_obj_one = worker.Worker(
                name='test_worker_one_name',
            )
            setattr(worker_obj_one, report_type, 10)
            worker_obj_two = worker.Worker(
                name='test_worker_two_name',
            )
            setattr(worker_obj_two, report_type, 10)

            self.assertEqual(
                first=getattr(host_obj, report_type),
                second=0,
            )

            host_obj.workers.append(worker_obj_one)

            self.assertEqual(
                first=getattr(host_obj, report_type),
                second=10,
            )

            host_obj.workers.append(worker_obj_two)

            self.assertEqual(
                first=getattr(host_obj, report_type),
                second=20,
            )

    def test_reports_per_second(self):
        for benchmark_type in (
            ('success_per_second', 'success',),
            ('failure_per_second', 'failure',),
            ('retry_per_second', 'retry',),
        ):
            host_obj = host.Host(
                name='test_host_name',
            )
            worker_obj = worker.Worker(
                name='test_worker_one_name',
            )

            self.assertEqual(
                first=getattr(host_obj, benchmark_type[0]),
                second=0,
            )

            host_obj.workers.append(worker_obj)

            self.assertEqual(
                first=getattr(host_obj, benchmark_type[0]),
                second=0,
            )

            for i in range(3):
                worker_obj.last_reports.append(
                    {
                        'host_name': host_obj.name,
                        'worker_name': worker_obj.name,
                        'type': benchmark_type[1],
                        'date': datetime.datetime.utcnow(),
                    }
                )

            self.assertEqual(
                first=getattr(host_obj, benchmark_type[0]),
                second=3,
            )

            time.sleep(1.1)

            self.assertEqual(
                first=getattr(host_obj, benchmark_type[0]),
                second=0,
            )
