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

    def ensure_host(self, host):
        for current_host in self.hosts:
            if host == current_host:
                break
        else:
            self.hosts.append(host)

    def ensure_worker(self, host, worker):
        report_time = datetime.datetime.utcnow()

        for current_host in self.hosts:
            if host == current_host:
                for current_worker in current_host.workers:
                    if current_worker == worker:
                        current_worker.last_seen = report_time

                        break
                else:
                    current_host.workers.append(worker)
                    worker.last_seen = report_time

    def report_worker(self, host, worker, message):
        for current_host in self.hosts:
            if host == current_host:
                for current_worker in current_host.workers:
                    if current_worker == worker:
                        current_worker.report(
                            message=message,
                        )

    @property
    def workers(self):
        workers = []

        for host in self.hosts:
            for worker in host.workers:
                workers.append(
                    {
                        'host': host,
                        'worker': worker,
                    }
                )

        return workers

    def get_statistics(self, statistics_type):
        value = 0

        for host in self.hosts:
            value += host.get_statistics(
                statistics_type=statistics_type,
            )

        return value

    @property
    def statistics(self):
        hosts = [
            {
                'name': host.name,
                'status': host.status,
            }
            for host in self.hosts
        ]

        workers = [
            {
                'hostname': worker['host'].name,
                'name': worker['worker'].name,
                'status': worker['worker'].status,
                'metrics': {
                    'failure': worker['worker'].get_statistics('failure'),
                    'retry': worker['worker'].get_statistics('retry'),
                    'success': worker['worker'].get_statistics('success'),
                    'process': worker['worker'].get_statistics('process'),
                }
            }
            for worker in self.workers
        ]

        statistics = {
            'hosts': hosts,
            'workers': workers,
            'metrics': {
                'success': self.get_statistics(
                    statistics_type='success',
                ),
                'failure': self.get_statistics(
                    statistics_type='failure',
                ),
                'retry': self.get_statistics(
                    statistics_type='retry',
                ),
                'process': self.get_statistics(
                    statistics_type='process',
                ),
                'heartbeat': self.get_statistics(
                    statistics_type='heartbeat',
                ),
            },
        }

        return statistics
