import unittest
import time
import requests

from .. import profiler


class ProfilerTestCase(
    unittest.TestCase,
):
    def setUp(
        self,
    ):
        self.profiler = profiler.profiler.Profiler()

    @staticmethod
    def http_request_func_to_profile(
        time_to_wait,
    ):
        time.sleep(time_to_wait)

        requests.get('https://www.google.com')

    def test_profiling_results(
        self,
    ):
        methods_count = 5
        http_request_func_to_profile_time_to_wait = 3

        self.profiler.start()

        self.http_request_func_to_profile(
            time_to_wait=http_request_func_to_profile_time_to_wait,
        )

        self.profiler.stop()

        num_of_profiled_methods = len(self.profiler.profiler.getstats())
        self.assertGreater(
            num_of_profiled_methods,
            methods_count,
            msg=f'profiler collected statistics count is lower than {num_of_profiled_methods}',
        )

        profiling_statistics = self.profiler.profiling_results(
            num_of_slowest_methods=methods_count,
        )['slowest_methods_profiles']
        self.assertEqual(
            len(profiling_statistics),
            methods_count,
            msg='profiler collected statistics count is not equal to the specified count limit',
        )
        for method_profile in profiling_statistics:
            if method_profile['method'] == 'http_request_func_to_profile':
                http_request_func_profile = method_profile
                break

        self.assertEqual(
            http_request_func_profile['line_number'],
            16,
            msg='line number is incorrect',
        )
        self.assertGreater(
            http_request_func_profile['total_time'],
            http_request_func_to_profile_time_to_wait,
            msg='time consumed is incorrect',
        )
        self.assertTrue(
            str.endswith(http_request_func_profile['filename'], '.py'),
            msg='profiling method filename is invalid',
        )

        for method_profile in profiling_statistics:
            if 'sleep' in method_profile['method']:
                sleep_func_profile = method_profile
                break

        self.assertGreater(
            sleep_func_profile['inline_time'],
            http_request_func_to_profile_time_to_wait,
            msg='time consumed is incorrect',
        )