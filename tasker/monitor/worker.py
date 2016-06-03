import datetime


class Worker:
    online_timedelta = datetime.timedelta(
        minutes=1,
    )

    def __init__(self, name):
        self.name = name
        self.statistics = {
            'success': 0,
            'failure': 0,
            'retry': 0,
            'process': 0,
            'heartbeat': 0,
        }
        self.last_seen = datetime.datetime.utcnow()

    @property
    def status(self):
        if self.last_seen + self.online_timedelta > datetime.datetime.utcnow():
            return 'online'
        else:
            return 'offline'

    def get_statistics(self, statistics_type):
        return self.statistics[statistics_type]

    def report(self, message):
        message_type = message.message_type
        message_value = message.message_value

        self.statistics[message_type.name] += message_value
        self.last_seen = datetime.datetime.utcnow()

    def __eq__(self, other):
        if other.name == self.name:
            return True
        else:
            return False
