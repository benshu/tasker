import zmq

from . import _connector


class Connector(_connector.Connector):
    '''
    '''
    def __init__(self, push_address, pull_address):
        self.push_address = push_address
        self.pull_address = pull_address

        self.zmq_context = zmq.Context()

        self.pull_socket = self.zmq_context.socket(zmq.PULL)
        self.pull_socket.connect(self.push_address)

        self.push_socket = self.zmq_context.socket(zmq.PUSH)
        self.push_socket.connect(self.pull_address)

        self.tasks_in_queue = 0

    def pop(self, key, timeout=0):
        '''
        '''
        message = self.pull_socket.recv()

        self.tasks_in_queue -= 1

        return message

    def pop_bulk(self, key, count):
        '''
        '''
        messages = []

        for i in range(count):
            message = self.pop(
                key=key,
                timeout=0,
            )
            messages.append(message)

        return messages

    def push(self, key, value):
        '''
        '''
        self.push_socket.send(value)

        self.tasks_in_queue += 1

    def push_bulk(self, key, values):
        '''
        '''
        for value in values:
            self.push(
                key=key,
                value=value,
            )

    def len(self, key):
        '''
        '''
        return self.tasks_in_queue

    def delete(self, key):
        '''
        '''
        pass

    def __getstate__(self):
        '''
        '''
        state = {
            'push_address': self.push_address,
            'pull_address': self.pull_address,
        }

        return state

    def __setstate__(self, value):
        '''
        '''
        self.__init__(
            push_address=value['push_address'],
            pull_address=value['pull_address'],
        )
