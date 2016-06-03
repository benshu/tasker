import unittest
import datetime

from .. import host
from .. import worker
from .. import message


class HostTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

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
            message.MessageType.process.name,
            message.MessageType.success.name,
            message.MessageType.failure.name,
            message.MessageType.retry.name,
            message.MessageType.heartbeat.name,
        ):
            host_obj = host.Host(
                name='test_host_name',
            )
            worker_obj_one = worker.Worker(
                name='test_worker_one_name',
            )
            worker_obj_one.statistics[report_type] = 10
            worker_obj_two = worker.Worker(
                name='test_worker_two_name',
            )
            worker_obj_two.statistics[report_type] = 10

            self.assertEqual(
                first=host_obj.get_statistics(report_type),
                second=0,
            )

            host_obj.workers.append(worker_obj_one)

            self.assertEqual(
                first=host_obj.get_statistics(report_type),
                second=10,
            )

            host_obj.workers.append(worker_obj_two)

            self.assertEqual(
                first=host_obj.get_statistics(report_type),
                second=20,
            )
