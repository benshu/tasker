import datetime
import enum
import elasticsearch


class MessageType(enum.Enum):
    success = 1
    failure = 2
    retry = 3


class MonitoringMessage:
    def __init__(self, host_name, worker_name, message_type, date):
        self.host_name = host_name
        self.worker_name = worker_name
        self.message_type = message_type
        self.date = date

    @property
    def encoded(self):
        message = {
            'host_name': self.host_name,
            'worker_name': self.worker_name,
            'type': self.message_type.value,
            'date': self.date,
        }

        return message


class MonitoringClient:
    '''
    '''
    index_name = 'monitoring'

    def __init__(self, monitoring_server, host_name, worker_name):
        self.host_name = host_name
        self.worker_name = worker_name

        self.elasticsearch_instance = elasticsearch.Elasticsearch(
            hosts=[
                {
                    'host': monitoring_server['host'],
                    'port': monitoring_server['port'],
                }
            ],
        )

    def _send_stats(self, message_type):
        message = MonitoringMessage(
            host_name=self.host_name,
            worker_name=self.worker_name,
            message_type=MessageType[message_type],
            date=datetime.datetime.utcnow(),
        )

        index_result = self.elasticsearch_instance.index(
            index=self.index_name,
            doc_type='metric',
            body=message.encoded,
        )

        return index_result['created']

    def send_success(self):
        self._send_stats(
            message_type='success',
        )

    def send_failure(self):
        self._send_stats(
            message_type='failure',
        )

    def send_retry(self):
        self._send_stats(
            message_type='retry',
        )
