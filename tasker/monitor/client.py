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

    def send_stats(self, message_type):
        message_obj = message.Message(
            hostname=self.host_name,
            worker_name=self.worker_name,
            message_type=message_type,
            date=datetime.datetime.utcnow(),
        )
        message_data = message_obj.serialize()

        try:
            statistics_socket = socket.socket(
                family=socket.AF_INET,
                type=socket.SOCK_STREAM,
            )
            statistics_socket.connect(
                (
                    self.stats_server['host'],
                    self.stats_server['port'],
                ),
            )
            statistics_socket.sendall(message_data)
        except Exception as exception:
            print(exception)
        finally:
            statistics_socket.close()

    def send_success(self):
        self.send_stats(
            message_type=message.MessageType.success,
        )

    def send_failure(self):
        self.send_stats(
            message_type=message.MessageType.failure,
        )

    def send_retry(self):
        self.send_stats(
            message_type=message.MessageType.retry,
        )

    def send_process(self):
        self.send_stats(
            message_type=message.MessageType.process,
        )

    def send_heartbeat(self):
        self.send_stats(
            message_type=message.MessageType.heartbeat,
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
