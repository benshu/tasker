import socket
import pickle
import datetime


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

    def send_stats(self, message_type):
        message = {
            'host_name': self.host_name,
            'worker_name': self.worker_name,
            'type': message_type,
            'date': datetime.datetime.utcnow(),
        }

        message_pickled = pickle.dumps(message)

        self.statistics_socket.sendto(
            message_pickled,
            (
                self.stats_server['host'],
                self.stats_server['port'],
            ),
        )

    def send_success(self):
        self.send_stats(
            message_type='success',
        )

    def send_failure(self):
        self.send_stats(
            message_type='failure',
        )

    def send_retry(self):
        self.send_stats(
            message_type='retry',
        )
