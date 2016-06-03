import socket
import datetime

from . import message


class StatisticsClient:
    '''
    '''
    def __init__(self, stats_server, host_name, worker_name):
        self.stats_server = stats_server
        self.host_name = host_name
        self.worker_name = worker_name

        self.statistics_socket = socket.socket(
            family=socket.AF_INET,
            type=socket.SOCK_DGRAM,
        )

    def increment_stats(self, message_type, message_value):
        message_obj = message.Message(
            hostname=self.host_name,
            worker_name=self.worker_name,
            message_type=message_type,
            message_value=message_value,
            date=datetime.datetime.utcnow(),
        )
        message_data = message_obj.serialize()

        self.statistics_socket.sendto(
            message_data,
            (
                self.stats_server['host'],
                self.stats_server['port'],
            ),
        )

    def increment_success(self, value=1):
        self.increment_stats(
            message_type=message.MessageType.success,
            message_value=value,
        )

    def increment_failure(self, value=1):
        self.increment_stats(
            message_type=message.MessageType.failure,
            message_value=value,
        )

    def increment_retry(self, value=1):
        self.increment_stats(
            message_type=message.MessageType.retry,
            message_value=value,
        )

    def increment_process(self, value=1):
        self.increment_stats(
            message_type=message.MessageType.process,
            message_value=value,
        )

    def increment_heartbeat(self, value=1):
        self.increment_stats(
            message_type=message.MessageType.heartbeat,
            message_value=value,
        )

    def __getstate__(self):
        '''
        '''
        state = {
            'stats_server': self.stats_server,
            'host_name': self.host_name,
            'worker_name': self.worker_name,
        }

        return state

    def __setstate__(self, value):
        '''
        '''
        self.__init__(
            stats_server=value['stats_server'],
            host_name=value['host_name'],
            worker_name=value['worker_name'],
        )
