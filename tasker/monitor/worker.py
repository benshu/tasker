import datetime
import collections


class Worker:
    def __init__(self, name):
        self.name = name
        self.start_time = datetime.datetime.utcnow()
        self.last_seen = datetime.datetime.utcfromtimestamp(0)
        self.statistics = {
            'success': 0,
            'failure': 0,
            'retry': 0,
        }
        self.last_reports = collections.deque(
            maxlen=1000,
        )

    @property
    def run_time(self):
        now_time = datetime.datetime.utcnow()
        time_delta = now_time - self.start_time

        return time_delta.total_seconds()

    def report(self, message):
        message_type = message['type']

        if message_type == 'success':
            self.success += 1
        elif message_type == 'failure':
            self.failure += 1
        elif message_type == 'retry':
            self.retry += 1
        else:
            pass

        self.last_reports.append(message)

    @property
    def success_per_second(self):
        return self.report_type_per_second(
            report_type='success',
            from_last_seconds=3,
        )

    @property
    def failure_per_second(self):
        return self.report_type_per_second(
            report_type='failure',
            from_last_seconds=3,
        )

    @property
    def retry_per_second(self):
        return self.report_type_per_second(
            report_type='retry',
            from_last_seconds=3,
        )

    def report_type_per_second(self, report_type, from_last_seconds):
        num_of_reports = 0

        report_diff_time = datetime.timedelta(
            seconds=from_last_seconds,
        )
        now_date = datetime.datetime.utcnow()

        for report in self.last_reports:
            if now_date - report['date'] < report_diff_time and report['type'] == report_type:
                num_of_reports += 1

        return num_of_reports

    @property
    def last_seen(self):
        return self._last_seen

    @last_seen.setter
    def last_seen(self, value):
        self._last_seen = value

    @property
    def success(self):
        return self.statistics['success']

    @success.setter
    def success(self, value):
        self.statistics['success'] = value

    @property
    def success_per_minute(self):
        run_time = self.run_time
        run_time_minutes = run_time / 60

        return self.statistics['success'] / run_time_minutes

    @property
    def failure(self):
        return self.statistics['failure']

    @failure.setter
    def failure(self, value):
        self.statistics['failure'] = value

    @property
    def failure_per_minute(self):
        run_time = self.run_time
        run_time_minutes = run_time / 60

        return self.statistics['failure'] / run_time_minutes

    @property
    def retry(self):
        return self.statistics['retry']

    @retry.setter
    def retry(self, value):
        self.statistics['retry'] = value

    @property
    def retry_per_minute(self):
        run_time = self.run_time
        run_time_minutes = run_time / 60

        return self.statistics['retry'] / run_time_minutes

    def __eq__(self, other):
        if other.name == self.name:
            return True
        else:
            return False
