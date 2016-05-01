import datetime


class Statistics:
    online_host_timedelta = datetime.timedelta(
        minutes=5,
    )
    online_worker_timedelta = datetime.timedelta(
        minutes=5,
    )

    def __init__(self):
        self.hosts = []

    def ensure_host(self, host_obj):
        for current_host in self.hosts:
            if host_obj == current_host:
                break
        else:
            self.hosts.append(host_obj)

    def ensure_worker(self, host_obj, worker_obj):
        report_time = datetime.datetime.now()

        for current_host in self.hosts:
            if host_obj == current_host:
                for current_worker in current_host.workers:
                    if current_worker == worker_obj:
                        current_worker.last_seen = report_time

                        break
                else:
                    current_host.workers.append(worker_obj)
                    worker_obj.last_seen = report_time

    def report_worker(self, host_obj, worker_obj, message_obj):
        for current_host in self.hosts:
            if host_obj == current_host:
                for current_worker in current_host.workers:
                    if current_worker == worker_obj:
                        current_worker.report(
                            message_obj=message_obj,
                        )

    @property
    def online_hosts(self):
        online_hosts = []

        for host_obj in self.hosts:
            if host_obj.last_seen + self.online_host_timedelta > datetime.datetime.now():
                online_hosts.append(host_obj)

        return online_hosts

    @property
    def online_workers(self):
        online_workers = []

        for host_obj in self.hosts:
            for worker_obj in host_obj.workers:
                if worker_obj.last_seen + self.online_worker_timedelta > datetime.datetime.now():
                    online_workers.append(
                        {
                            'host': host_obj,
                            'worker': worker_obj,
                        }
                    )

        return online_workers

    @property
    def success(self):
        success = 0

        for host_obj in self.hosts:
            success += host_obj.success

        return success

    @property
    def success_per_second(self):
        success_per_second = 0

        for host_obj in self.hosts:
            success_per_second += host_obj.success_per_second

        return success_per_second

    @property
    def success_per_minute(self):
        success_per_minute = 0

        for host_obj in self.hosts:
            success_per_minute += host_obj.success_per_minute

        return success_per_minute

    @property
    def failure(self):
        failure = 0

        for host_obj in self.hosts:
            failure += host_obj.failure

        return failure

    @property
    def failure_per_second(self):
        failure_per_second = 0

        for host_obj in self.hosts:
            failure_per_second += host_obj.failure_per_second

        return failure_per_second

    @property
    def failure_per_minute(self):
        failure_per_minute = 0

        for host_obj in self.hosts:
            failure_per_minute += host_obj.failure_per_minute

        return failure_per_minute

    @property
    def retry(self):
        retry = 0

        for host_obj in self.hosts:
            retry += host_obj.retry

        return retry

    @property
    def retry_per_second(self):
        retry_per_second = 0

        for host_obj in self.hosts:
            retry_per_second += host_obj.retry_per_second

        return retry_per_second

    @property
    def retry_per_minute(self):
        retry_per_minute = 0

        for host_obj in self.hosts:
            retry_per_minute += host_obj.retry_per_minute

        return retry_per_minute

    @property
    def process(self):
        process = 0

        for host_obj in self.hosts:
            process += host_obj.process

        return process

    @property
    def process_per_second(self):
        process_per_second = 0

        for host_obj in self.hosts:
            process_per_second += host_obj.process_per_second

        return process_per_second

    @property
    def process_per_minute(self):
        process_per_minute = 0

        for host_obj in self.hosts:
            process_per_minute += host_obj.process_per_minute

        return process_per_minute

    @property
    def heartbeat(self):
        heartbeat = 0

        for host_obj in self.hosts:
            heartbeat += host_obj.heartbeat

        return heartbeat

    @property
    def heartbeat_per_second(self):
        heartbeat_per_second = 0

        for host_obj in self.hosts:
            heartbeat_per_second += host_obj.heartbeat_per_second

        return heartbeat_per_second

    @property
    def heartbeat_per_minute(self):
        heartbeat_per_minute = 0

        for host_obj in self.hosts:
            heartbeat_per_minute += host_obj.heartbeat_per_minute

        return heartbeat_per_minute

    @property
    def all(self):
        online_hosts = [
            host_obj.name
            for host_obj in self.online_hosts
        ]

        online_workers = [
            {
                'name': '{worker_name}@{host_name}'.format(
                    worker_name=worker_obj['worker'].name,
                    host_name=worker_obj['host'].name,
                ),
                'failure': worker_obj['worker'].failure,
                'retry': worker_obj['worker'].retry,
                'success': worker_obj['worker'].success,
            }
            for worker_obj in self.online_workers
        ]

        statistics = {
            'online_hosts': online_hosts,
            'online_workers': online_workers,
            'success': self.success,
            'failure': self.failure,
            'retry': self.retry,
            'process': self.process,
            'heartbeat': self.heartbeat,
            'success_per_minute': self.success_per_minute,
            'failure_per_minute': self.failure_per_minute,
            'retry_per_minute': self.retry_per_minute,
            'process_per_minute': self.process_per_minute,
            'heartbeat_per_minute': self.heartbeat_per_minute,
            'success_per_second': self.success_per_second,
            'failure_per_second': self.failure_per_second,
            'retry_per_second': self.retry_per_second,
            'process_per_second': self.process_per_second,
            'heartbeat_per_second': self.heartbeat_per_second,
        }

        return statistics
