import unittest
import time
import os
import sys
import multiprocessing
import signal
import re

from .. import devices


class DummyMonitorClient:
    def __init__(self):
        self.counter = 0

    def increment_heartbeat(self):
        self.counter += 1


class DevicesTestCase(unittest.TestCase):
    def setUp(self):
        self.monitor_client = DummyMonitorClient()
        self.heartbeater = devices.heartbeater.Heartbeater(
            monitor_client=self.monitor_client,
            interval=1.0,
        )

        signal.signal(signal.SIGABRT, self.sigabrt_handler)
        signal.signal(signal.SIGINT, self.sigint_handler)

        self.sigint_fired = False
        self.sigabrt_fired = False

    def sigabrt_handler(self, signal_num, frame):
        self.sigabrt_fired = True

    def sigint_handler(self, signal_num, frame):
        '''
        '''
        self.sigint_fired = True

    def test_heartbeater(self):
        self.heartbeater.start()
        self.assertEqual(self.monitor_client.counter, 0)
        time.sleep(1.2)
        self.assertEqual(self.monitor_client.counter, 1)
        time.sleep(1.2)
        self.assertEqual(self.monitor_client.counter, 2)
        self.heartbeater.stop()
        time.sleep(1.2)
        self.assertEqual(self.monitor_client.counter, 2)

    def test_local_killer(self):
        local_killer = devices.killer.LocalKiller(
            pid=os.getpid(),
            soft_timeout=1.0,
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=3.0,
            hard_timeout_signal=signal.SIGABRT,
            critical_timeout=5.0,
            critical_timeout_signal=signal.SIGTERM,
        )

        local_killer.start()
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)
        time.sleep(1.2)
        self.assertFalse(self.sigabrt_fired)
        self.assertTrue(self.sigint_fired)
        time.sleep(1.2)
        self.assertFalse(self.sigabrt_fired)
        self.assertTrue(self.sigint_fired)
        time.sleep(1.2)
        self.assertTrue(self.sigabrt_fired)
        self.assertTrue(self.sigint_fired)
        local_killer.stop()

        self.sigint_fired = False
        self.sigabrt_fired = False
        time.sleep(1.2)
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)
        time.sleep(1.2)
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)

        local_killer.reset()
        local_killer.start()
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)
        time.sleep(0.5)
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)
        local_killer.reset()
        time.sleep(0.5)
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)
        local_killer.reset()
        time.sleep(0.5)
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)
        local_killer.reset()
        time.sleep(0.5)
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)
        local_killer.reset()
        local_killer.stop()
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)

    def test_remote_killer(self):
        remote_killer = devices.killer.RemoteKiller(
            pid=os.getpid(),
            soft_timeout=1.0,
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=3.0,
            hard_timeout_signal=signal.SIGABRT,
            critical_timeout=5.0,
            critical_timeout_signal=signal.SIGTERM,
        )

        remote_killer.start()
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)
        time.sleep(1.2)
        self.assertFalse(self.sigabrt_fired)
        self.assertTrue(self.sigint_fired)
        time.sleep(1.2)
        self.assertFalse(self.sigabrt_fired)
        self.assertTrue(self.sigint_fired)
        time.sleep(1.2)
        self.assertTrue(self.sigabrt_fired)
        self.assertTrue(self.sigint_fired)
        remote_killer.stop()

        self.sigint_fired = False
        self.sigabrt_fired = False
        time.sleep(1.2)
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)
        time.sleep(1.2)
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)

        remote_killer.reset()
        remote_killer.start()
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)
        time.sleep(0.5)
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)
        remote_killer.reset()
        time.sleep(0.5)
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)
        remote_killer.reset()
        time.sleep(0.5)
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)
        remote_killer.reset()
        time.sleep(0.5)
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)
        remote_killer.reset()
        remote_killer.stop()
        self.assertFalse(self.sigabrt_fired)
        self.assertFalse(self.sigint_fired)

    def test_sleep_case_local_killer(self):
        test_process_obj = TestProcess()
        sleeping_process = multiprocessing.Process(
            target=test_process_obj.sleep,
            kwargs={
                'interval': 30,
            },
        )
        sleeping_process.start()

        self.assertTrue(sleeping_process.is_alive())

        local_killer = devices.killer.LocalKiller(
            pid=sleeping_process.pid,
            soft_timeout=1.0,
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=2.0,
            hard_timeout_signal=signal.SIGABRT,
            critical_timeout=5.0,
            critical_timeout_signal=signal.SIGTERM,
        )

        local_killer.start()
        self.assertTrue(sleeping_process.is_alive())
        time.sleep(1.2)
        self.assertFalse(sleeping_process.is_alive())
        self.assertEqual(sleeping_process.exitcode, 20)

    def test_no_int_case_local_killer(self):
        test_process_obj = TestProcess()
        sleeping_process = multiprocessing.Process(
            target=test_process_obj.no_int_sleep,
            kwargs={
                'interval': 30,
            },
        )
        sleeping_process.start()

        self.assertTrue(sleeping_process.is_alive())

        local_killer = devices.killer.LocalKiller(
            pid=sleeping_process.pid,
            soft_timeout=1.0,
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=2.0,
            hard_timeout_signal=signal.SIGABRT,
            critical_timeout=5.0,
            critical_timeout_signal=signal.SIGTERM,
        )

        local_killer.start()
        self.assertTrue(sleeping_process.is_alive())
        time.sleep(1.2)
        self.assertTrue(sleeping_process.is_alive())
        time.sleep(1.2)
        self.assertFalse(sleeping_process.is_alive())
        self.assertEqual(sleeping_process.exitcode, 10)

    def test_sleep_case_remote_killer(self):
        test_process_obj = TestProcess()
        sleeping_process = multiprocessing.Process(
            target=test_process_obj.sleep,
            kwargs={
                'interval': 30,
            },
        )
        sleeping_process.start()

        self.assertTrue(sleeping_process.is_alive())

        remote_killer = devices.killer.RemoteKiller(
            pid=sleeping_process.pid,
            soft_timeout=1.0,
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=2.0,
            hard_timeout_signal=signal.SIGABRT,
            critical_timeout=5.0,
            critical_timeout_signal=signal.SIGTERM,
        )

        remote_killer.start()
        self.assertTrue(sleeping_process.is_alive())
        time.sleep(1.2)
        self.assertFalse(sleeping_process.is_alive())
        self.assertEqual(sleeping_process.exitcode, 20)

    def test_no_int_case_remote_killer(self):
        test_process_obj = TestProcess()
        sleeping_process = multiprocessing.Process(
            target=test_process_obj.no_int_sleep,
            kwargs={
                'interval': 30,
            },
        )
        sleeping_process.start()

        self.assertTrue(sleeping_process.is_alive())

        remote_killer = devices.killer.RemoteKiller(
            pid=sleeping_process.pid,
            soft_timeout=1.0,
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=2.0,
            hard_timeout_signal=signal.SIGABRT,
            critical_timeout=5.0,
            critical_timeout_signal=signal.SIGTERM,
        )

        remote_killer.start()
        self.assertTrue(sleeping_process.is_alive())
        time.sleep(1.2)
        self.assertTrue(sleeping_process.is_alive())
        time.sleep(1.2)
        self.assertFalse(sleeping_process.is_alive())
        self.assertEqual(sleeping_process.exitcode, 10)

    def test_lost_case_remote_killer(self):
        test_process_obj = TestProcess()
        sleeping_process = multiprocessing.Process(
            target=test_process_obj.lost,
            kwargs={
                'interval': 30,
            },
        )
        sleeping_process.start()

        self.assertTrue(sleeping_process.is_alive())

        remote_killer = devices.killer.RemoteKiller(
            pid=sleeping_process.pid,
            soft_timeout=1.0,
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=2.0,
            hard_timeout_signal=signal.SIGABRT,
            critical_timeout=3.0,
            critical_timeout_signal=signal.SIGTERM,
        )

        remote_killer.start()
        self.assertTrue(sleeping_process.is_alive())
        time.sleep(1.2)
        self.assertTrue(sleeping_process.is_alive())
        time.sleep(1.2)
        self.assertTrue(sleeping_process.is_alive())
        time.sleep(1.2)
        self.assertFalse(sleeping_process.is_alive())
        self.assertEqual(sleeping_process.exitcode, -15)


class TestProcess:
    def __init__(self):
        '''
        '''
        signal.signal(signal.SIGABRT, self.sigabrt_handler)
        signal.signal(signal.SIGINT, self.sigint_handler)

    def sleep(self, interval):
        '''
        '''
        time.sleep(interval)

    def no_int_sleep(self, interval):
        '''
        '''
        signal.signal(signal.SIGINT, lambda a, b: True)
        time.sleep(interval)

    def lost(self, interval):
        '''
        '''
        signal.signal(signal.SIGINT, lambda a, b: True)
        signal.signal(signal.SIGABRT, lambda a, b: True)
        time.sleep(interval)

    def sigabrt_handler(self, signal_num, frame):
        '''
        '''
        sys.exit(10)

    def sigint_handler(self, signal_num, frame):
        '''
        '''
        sys.exit(20)

    def __setstate__(self, state):
        '''
        '''
        self.__init__()
