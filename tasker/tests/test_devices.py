import unittest
import time
import os
import sys
import multiprocessing
import signal

from .. import devices


class DummyMonitorClient:
    def __init__(self):
        self.counter = 0

    def increment_heartbeat(self):
        self.counter += 1


class DevicesTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    def setUp(self):
        self.sigint_fired = False
        self.sigabrt_fired = False

        signal.signal(signal.SIGABRT, self.sigabrt_handler)
        signal.signal(signal.SIGINT, self.sigint_handler)

    def sigabrt_handler(self, signal_num, frame):
        self.sigabrt_fired = True

    def sigint_handler(self, signal_num, frame):
        '''
        '''
        self.sigint_fired = True

    def test_heartbeater(self):
        monitor_client = DummyMonitorClient()
        heartbeater = devices.heartbeater.Heartbeater(
            monitor_client=monitor_client,
            interval=1.0,
        )
        heartbeater.start()
        self.assertEqual(monitor_client.counter, 0)
        time.sleep(1.2)
        self.assertEqual(monitor_client.counter, 1)
        time.sleep(1.2)
        self.assertEqual(monitor_client.counter, 2)
        heartbeater.stop()
        time.sleep(1.2)
        self.assertEqual(monitor_client.counter, 2)

    def test_memory_local_killer(self):
        local_killer = devices.killer.LocalKiller(
            pid=os.getpid(),
            soft_timeout=0.0,
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=0.0,
            hard_timeout_signal=signal.SIGABRT,
            critical_timeout=0.0,
            critical_timeout_signal=signal.SIGTERM,
            memory_limit=4 * 1024 * 1024 * 1024,
            memory_limit_signal=signal.SIGINT,
        )

        local_killer.start()
        self.sigint_fired = False
        self.assertFalse(self.sigint_fired)
        mem_buffer = ' ' * (3 * 1024 * 1024 * 1024)
        time.sleep(1)
        self.assertFalse(self.sigint_fired)
        mem_buffer += ' ' * (1 * 1024 * 1024 * 1024)
        time.sleep(1)
        self.assertTrue(self.sigint_fired)
        local_killer.stop()

    def test_memory_remote_killer(self):
        local_killer = devices.killer.RemoteKiller(
            pid=os.getpid(),
            soft_timeout=0.0,
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=0.0,
            hard_timeout_signal=signal.SIGABRT,
            critical_timeout=0.0,
            critical_timeout_signal=signal.SIGTERM,
            memory_limit=4 * 1024 * 1024 * 1024,
            memory_limit_signal=signal.SIGINT,
        )

        local_killer.start()
        self.sigint_fired = False
        self.assertFalse(self.sigint_fired)
        mem_buffer = ' ' * (3 * 1024 * 1024 * 1024)
        time.sleep(1)
        self.assertFalse(self.sigint_fired)
        mem_buffer += ' ' * (1 * 1024 * 1024 * 1024)
        time.sleep(1)
        self.assertTrue(self.sigint_fired)
        local_killer.stop()

    def test_timeouts_local_killer(self):
        local_killer = devices.killer.LocalKiller(
            pid=os.getpid(),
            soft_timeout=1.0,
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=3.0,
            hard_timeout_signal=signal.SIGABRT,
            critical_timeout=5.0,
            critical_timeout_signal=signal.SIGTERM,
            memory_limit=4 * 1024 * 1024 * 1024,
            memory_limit_signal=signal.SIGINT,
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

    def test_timeouts_remote_killer(self):
        remote_killer = devices.killer.RemoteKiller(
            pid=os.getpid(),
            soft_timeout=1.0,
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=3.0,
            hard_timeout_signal=signal.SIGABRT,
            critical_timeout=5.0,
            critical_timeout_signal=signal.SIGTERM,
            memory_limit=4 * 1024 * 1024 * 1024,
            memory_limit_signal=signal.SIGINT,
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
        testing_process = multiprocessing.Process(
            target=test_process_obj.sleep,
            kwargs={
                'interval': 30,
            },
        )
        testing_process.start()

        self.assertTrue(testing_process.is_alive())

        local_killer = devices.killer.LocalKiller(
            pid=testing_process.pid,
            soft_timeout=1.0,
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=2.0,
            hard_timeout_signal=signal.SIGABRT,
            critical_timeout=5.0,
            critical_timeout_signal=signal.SIGTERM,
            memory_limit=4 * 1024 * 1024 * 1024,
            memory_limit_signal=signal.SIGINT,
        )

        local_killer.start()
        self.assertTrue(testing_process.is_alive())
        time.sleep(1.2)
        self.assertFalse(testing_process.is_alive())
        self.assertEqual(testing_process.exitcode, 20)
        local_killer.stop()

    def test_no_int_case_local_killer(self):
        test_process_obj = TestProcess()
        testing_process = multiprocessing.Process(
            target=test_process_obj.no_int_sleep,
            kwargs={
                'interval': 30,
            },
        )
        testing_process.daemon = True
        testing_process.start()

        self.assertTrue(testing_process.is_alive())

        local_killer = devices.killer.LocalKiller(
            pid=testing_process.pid,
            soft_timeout=1.0,
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=2.0,
            hard_timeout_signal=signal.SIGABRT,
            critical_timeout=5.0,
            critical_timeout_signal=signal.SIGTERM,
            memory_limit=4 * 1024 * 1024 * 1024,
            memory_limit_signal=signal.SIGINT,
        )

        local_killer.start()
        self.assertTrue(testing_process.is_alive())
        time.sleep(1.2)
        self.assertTrue(testing_process.is_alive())
        time.sleep(1.2)
        self.assertFalse(testing_process.is_alive())
        self.assertEqual(testing_process.exitcode, 10)
        local_killer.stop()

    def test_sleep_case_remote_killer(self):
        test_process_obj = TestProcess()
        testing_process = multiprocessing.Process(
            target=test_process_obj.sleep,
            kwargs={
                'interval': 30,
            },
        )
        testing_process.daemon = True
        testing_process.start()

        self.assertTrue(testing_process.is_alive())

        remote_killer = devices.killer.RemoteKiller(
            pid=testing_process.pid,
            soft_timeout=1.0,
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=2.0,
            hard_timeout_signal=signal.SIGABRT,
            critical_timeout=5.0,
            critical_timeout_signal=signal.SIGTERM,
            memory_limit=4 * 1024 * 1024 * 1024,
            memory_limit_signal=signal.SIGINT,
        )

        remote_killer.start()
        self.assertTrue(testing_process.is_alive())
        time.sleep(1.2)
        self.assertFalse(testing_process.is_alive())
        self.assertEqual(testing_process.exitcode, 20)
        remote_killer.stop()

    def test_no_int_case_remote_killer(self):
        test_process_obj = TestProcess()
        testing_process = multiprocessing.Process(
            target=test_process_obj.no_int_sleep,
            kwargs={
                'interval': 30,
            },
        )
        testing_process.daemon = True
        testing_process.start()

        self.assertTrue(testing_process.is_alive())

        remote_killer = devices.killer.RemoteKiller(
            pid=testing_process.pid,
            soft_timeout=1.0,
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=2.0,
            hard_timeout_signal=signal.SIGABRT,
            critical_timeout=5.0,
            critical_timeout_signal=signal.SIGTERM,
            memory_limit=4 * 1024 * 1024 * 1024,
            memory_limit_signal=signal.SIGINT,
        )

        remote_killer.start()
        self.assertTrue(testing_process.is_alive())
        time.sleep(1.2)
        self.assertTrue(testing_process.is_alive())
        time.sleep(1.2)
        self.assertFalse(testing_process.is_alive())
        self.assertEqual(testing_process.exitcode, 10)
        remote_killer.stop()

    def test_lost_case_remote_killer(self):
        test_process_obj = TestProcess()
        testing_process = multiprocessing.Process(
            target=test_process_obj.lost,
            kwargs={
                'interval': 30,
            },
        )
        testing_process.daemon = True
        testing_process.start()

        self.assertTrue(testing_process.is_alive())

        remote_killer = devices.killer.RemoteKiller(
            pid=testing_process.pid,
            soft_timeout=1.0,
            soft_timeout_signal=signal.SIGINT,
            hard_timeout=2.0,
            hard_timeout_signal=signal.SIGABRT,
            critical_timeout=3.0,
            critical_timeout_signal=signal.SIGTERM,
            memory_limit=4 * 1024 * 1024 * 1024,
            memory_limit_signal=signal.SIGINT,
        )

        remote_killer.start()
        self.assertTrue(testing_process.is_alive())
        time.sleep(1.2)
        self.assertTrue(testing_process.is_alive())
        time.sleep(1.2)
        self.assertTrue(testing_process.is_alive())
        time.sleep(1.2)
        self.assertFalse(testing_process.is_alive())
        self.assertEqual(testing_process.exitcode, -15)
        remote_killer.stop()


class TestProcess:
    def init(self):
        '''
        '''
        signal.signal(signal.SIGABRT, self.sigabrt_handler)
        signal.signal(signal.SIGINT, self.sigint_handler)

    def sleep(self, interval):
        '''
        '''
        self.init()
        time.sleep(interval)

    def no_int_sleep(self, interval):
        '''
        '''
        self.init()
        signal.signal(signal.SIGINT, lambda a, b: True)
        time.sleep(interval)

    def lost(self, interval):
        '''
        '''
        self.init()
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
        self.init()
