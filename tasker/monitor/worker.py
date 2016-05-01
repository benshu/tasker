import datetime
import collections

from . import message


class Worker:
    def __init__(self, name):
        self.name = name
        self.start_time = datetime.datetime.utcnow()
        self.last_seen = datetime.datetime.utcfromtimestamp(0)
        self.statistics = {
            'success': 0,
            'failure': 0,
            'retry': 0,
            'process': 0,
            'heartbeat': 0,
        }
        self.last_reports = collections.deque(
            maxlen=1000,
        )

    @property
    def run_time(self):
        now_time = datetime.datetime.utcnow()
        time_delta = now_time - self.start_time

        return time_delta.total_seconds()

    def report(self, message_obj):
        message_type = message_obj.message_type

        if message_type == message.MessageType.success:
            self.success += 1
        elif message_type == message.MessageType.failure:
            self.failure += 1
        elif message_type == message.MessageType.retry:
            self.retry += 1
        elif message_type == message.MessageType.process:
            self.process += 1
        elif message_type == message.MessageType.heartbeat:
            self.heartbeat += 1
        else:
            pass

        self.last_reports.append(message_obj)

    @property
    def success_per_second(self):
        return self.report_type_per_second(
            report_type='success',
        )

    @property
    def failure_per_second(self):
        return self.report_type_per_second(
            report_type='failure',
        )

    @property
    def retry_per_second(self):
        return self.report_type_per_second(
            report_type='retry',
        )

    @property
    def process_per_second(self):
        return self.report_type_per_second(
            report_type='process',
        )

    @property
    def heartbeat_per_second(self):
        return self.report_type_per_second(
            report_type='heartbeat',
        )

    def report_type_per_second(self, report_type):
        num_of_reports = 0

        report_diff_time = datetime.timedelta(
            seconds=1,
        )
        now_date = datetime.datetime.utcnow()

        for report in self.last_reports:
            if now_date - report.date < report_diff_time and report.message_type.name == report_type:
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

    @property
    def process(self):
        return self.statistics['process']

    @process.setter
    def process(self, value):
        self.statistics['process'] = value

    @property
    def process_per_minute(self):
        run_time = self.run_time
        run_time_minutes = run_time / 60

        return self.statistics['process'] / run_time_minutes

    @property
    def heartbeat(self):
        return self.statistics['heartbeat']

    @heartbeat.setter
    def heartbeat(self, value):
        self.statistics['heartbeat'] = value

    @property
    def heartbeat_per_minute(self):
        run_time = self.run_time
        run_time_minutes = run_time / 60

        return self.statistics['heartbeat'] / run_time_minutes

    def __eq__(self, other):
        if other.name == self.name:
            return True
        else:
            return False
