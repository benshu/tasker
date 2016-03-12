import datetime


class Host:
    def __init__(self, name):
        self.name = name
        self.workers = []

    @property
    def run_time(self):
        now_time = datetime.datetime.utcnow()
        start_time = now_time

        for worker in self.workers:
            if worker.start_time < start_time:
                start_time = worker.start_time

        run_time = now_time - start_time

        return run_time.total_seconds()

    @property
    def last_seen(self):
        last_seen = datetime.datetime.utcfromtimestamp(0)

        for worker in self.workers:
            if worker.last_seen > last_seen:
                last_seen = worker.last_seen

        return last_seen

    @property
    def success(self):
        success = 0

        for worker in self.workers:
            success += worker.success

        return success

    @property
    def success_per_second(self):
        success = 0

        for worker in self.workers:
            success += worker.success_per_second

        return success

    @property
    def success_per_minute(self):
        run_time = self.run_time
        run_time_minutes = run_time / 60

        return self.success / run_time_minutes

    @property
    def failure(self):
        failure = 0

        for worker in self.workers:
            failure += worker.failure

        return failure

    @property
    def failure_per_second(self):
        failure = 0

        for worker in self.workers:
            failure += worker.failure_per_second

        return failure

    @property
    def failure_per_minute(self):
        run_time = self.run_time
        run_time_minutes = run_time / 60

        return self.failure / run_time_minutes

    @property
    def retry(self):
        retry = 0

        for worker in self.workers:
            retry += worker.retry

        return retry

    @property
    def retry_per_second(self):
        retry = 0

        for worker in self.workers:
            retry += worker.retry_per_second

        return retry

    @property
    def retry_per_minute(self):
        run_time = self.run_time
        run_time_minutes = run_time / 60

        return self.retry / run_time_minutes

    def __eq__(self, other):
        if other.name == self.name:
            return True
        else:
            return False
