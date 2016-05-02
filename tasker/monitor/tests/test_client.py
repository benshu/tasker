import threading
import time
import unittest
import requests
import asyncio

from .. import client
from .. import server
from .. import statistics
from .. import message


class ServerThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self.event_loop = asyncio.new_event_loop()
        self.statistics_obj = statistics.Statistics()

    def run(self):
        self.statistics_server = server.StatisticsServer(
            event_loop=self.event_loop,
            web_server={
                'host': '127.0.0.1',
                'port': 8080,
            },
            udp_server={
                'host': '127.0.0.1',
                'port': 9999,
            },
            statistics_obj=self.statistics_obj,
        )

        self.event_loop.run_forever()


class MonitorClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server_thread = ServerThread()
        cls.server_thread.start()

        cls.client = client.StatisticsClient(
            stats_server={
                'host': '127.0.0.1',
                'port': 9999,
            },
            host_name='host_1',
            worker_name='worker_1',
        )

        while not cls.server_thread.event_loop.is_running():
            pass

    @classmethod
    def tearDownClass(cls):
        cls.server_thread.event_loop.call_soon_threadsafe(
            cls.server_thread.event_loop.stop,
        )

        while cls.server_thread.event_loop.is_running():
            pass

        cls.server_thread.statistics_server.close()
        cls.server_thread.event_loop.close()

    def get_stats(self):
        response = requests.get('http://127.0.0.1:8080/statistics')

        return response.json()

    def test_reported_stats(self):
        for message_type in (
            message.MessageType.process,
            message.MessageType.success,
            message.MessageType.failure,
            message.MessageType.retry,
            message.MessageType.heartbeat,
        ):
            current_stats = self.get_stats()

            self.assertEqual(
                first=current_stats[message_type.name],
                second=0,
                msg='statistics server {message_type} rate is not cleaned up'.format(
                    message_type=message_type.name,
                ),
            )

            self.client.send_stats(
                message_type=message_type,
            )
            current_stats = self.get_stats()

            self.assertEqual(
                first=current_stats[message_type.name],
                second=1,
                msg='statistics server {message_type} rate is not correct'.format(
                    message_type=message_type.name,
                ),
            )

            for i in range(10):
                time.sleep(0.1)
                self.client.send_stats(
                    message_type=message_type,
                )

            current_stats = self.get_stats()
            self.assertEqual(
                first=current_stats[message_type.name],
                second=11,
                msg='statistics server {message_type} rate is not correct'.format(
                    message_type=message_type.name,
                ),
            )
