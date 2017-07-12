import uuid
import cProfile


class Profiler:
    profiler = cProfile.Profile()

    def start(
        self,
    ):
        self.profiler.clear()
        self.profiler.enable()

    def stop(
        self,
    ):
        self.profiler.disable()

    def profiling_results(
        self,
        num_of_slowest_methods,
    ):
        profiling_results = []
        task_profiling_id = str(uuid.uuid4())

        for profiler_entry in self.profiler.getstats():
            external_library_code = isinstance(
                profiler_entry.code,
                str,
            )

            if external_library_code:
                method_name = profiler_entry.code
                filename = ''
                line_number = ''
                raw = profiler_entry.code
            else:
                method_name = profiler_entry.code.co_name
                filename = profiler_entry.code.co_filename
                line_number = profiler_entry.code.co_firstlineno
                raw = '{filename} | {line_number} | {method}'.format(
                    filename=filename,
                    line_number=line_number,
                    method=method_name,
                )

            parsed_profiler_entry = {
                'method': method_name,
                'filename': filename,
                'line_number': line_number,
                'inline_time': profiler_entry.inlinetime,
                'total_time': profiler_entry.totaltime,
                'raw': raw,
                'task_profiling_id': task_profiling_id,
                'call_count': profiler_entry.callcount,
            }

            profiling_results.append(parsed_profiler_entry)

        sorted_methods_by_totaltime = sorted(
            profiling_results,
            key=lambda x: x['total_time'],
            reverse=True,
        )

        slowest_methods_profiles = sorted_methods_by_totaltime[:num_of_slowest_methods]

        totaltime = sum(
            [
                entry['inline_time']
                for entry in profiling_results
            ]
        )

        profiling_results = {
            'slowest_methods_profiles': slowest_methods_profiles,
            'task_time_duration': totaltime,
        }

        return profiling_results
