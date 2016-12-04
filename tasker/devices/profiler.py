import time
import datetime

from .. import logger


class Profiler:
    '''
    '''
    def __init__(self):
        self.logger = logger.logger.Logger(
            logger_name='profiler',
        )

        self.statistics = {
            'tasks': {
                'total': 0,
                'last_sample': None,
                'profiling': {
                    'tasks_per_minute': 0,
                },
            },
        }

    def increment_total_tasks(self, number_of_tasks):
        '''
        '''
        self.statistics['tasks']['total'] += number_of_tasks

        current_sampling_time = datetime.datetime.now()
        last_sampling_time = self.statistics['tasks']['last_sample']

        if last_sampling_time:
            timedelta_between_sampling = current_sampling_time - last_sampling_time

            tasks_per_minute = number_of_tasks / (timedelta_between_sampling.total_seconds() / 60)
            self.statistics['tasks']['profiling']['tasks_per_minute'] = tasks_per_minute
        else:
            self.statistics['tasks']['last_sample'] = current_sampling_time
