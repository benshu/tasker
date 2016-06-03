import datetime


class Host:
    online_timedelta = datetime.timedelta(
        minutes=1,
    )

    def __init__(self, name):
        self.name = name
        self.workers = []

    @property
    def last_seen(self):
        last_seen = datetime.datetime.utcfromtimestamp(0)

        for worker in self.workers:
            if worker.last_seen > last_seen:
                last_seen = worker.last_seen

        return last_seen

    @property
    def status(self):
        if self.last_seen + self.online_timedelta > datetime.datetime.utcnow():
            return 'online'
        else:
            return 'offline'

    def get_statistics(self, statistics_type):
        value = 0

        for worker in self.workers:
            value += worker.get_statistics(
                statistics_type=statistics_type,
            )

        return value

    def __eq__(self, other):
        if other.name == self.name:
            return True
        else:
            return False
