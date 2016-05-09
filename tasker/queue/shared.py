import zmq
import queue

from . import _queue


class Queue(_queue.Queue):
    '''
    '''
    name = 'shared'

    def __init__(self, tasks_per_transaction, rep_port, *args, **kwargs):
        '''
        '''
        super().__init__(*args, **kwargs)

        self.tasks_per_transaction = tasks_per_transaction
        self.rep_port = rep_port

        context = zmq.Context()
        self.zmq_req_socket = context.socket(zmq.REQ)
        self.zmq_req_socket.connect(
            'tcp://127.0.0.1:{rep_port}'.format(
                rep_port=rep_port,
            )
        )

    def _dequeue_from_original(self):
        '''
        '''
        value = self.connector.pop(
            key=self.queue_name,
            timeeout=0,
        )

        return value

    def _dequeue_bulk_from_original(self):
        '''
        '''
        values = self.connector.pop_bulk(
            key=self.queue_name,
            count=self.tasks_per_transaction,
        )

        return values

    def _dequeue(self, timeout):
        '''
        '''
        try:
            self.zmq_req_socket.send(b'')
            value = self.zmq_req_socket.recv()

            return value
        except queue.Empty:
            return None

    def _dequeue_bulk(self, count):
        '''
        '''
        value = self._dequeue()

        return [value]

    def _enqueue(self, value):
        '''
        '''
        pushed = self.connector.push(
            key=self.queue_name,
            value=value,
        )

        return pushed

    def _enqueue_bulk(self, values):
        '''
        '''
        pushed = self.connector.push_bulk(
            key=self.queue_name,
            values=values,
        )

        return pushed

    def _add_result(self, value):
        '''
        '''
        added = self.connector.add_to_set(
            set_name=self.results_queue_name,
            value=value,
        )

        return added

    def _remove_result(self, value):
        '''
        '''
        removed = self.connector.remove_from_set(
            set_name=self.results_queue_name,
            value=value,
        )

        return removed

    def _has_result(self, value):
        '''
        '''
        is_in_set = self.connector.is_member_of_set(
            set_name=self.results_queue_name,
            value=value,
        )

        return is_in_set

    def _len(self):
        '''
        '''
        queue_len = self.connector.len(
            key=self.queue_name,
        )

        return queue_len

    def _flush(self):
        '''
        '''
        self.connector.delete(
            key=self.queue_name,
        )
        self.connector.delete(
            key=self.results_queue_name,
        )

    def __getstate__(self):
        '''
        '''
        state = {
            'tasks_per_transaction': self.tasks_per_transaction,
            'rep_port': self.rep_port,
            'queue_name': self.queue_name,
            'connector': self.connector,
            'encoder': self.encoder,
        }

        return state

    def __setstate__(self, value):
        '''
        '''
        self.__init__(
            tasks_per_transaction=value['tasks_per_transaction'],
            rep_port=value['rep_port'],
            queue_name=value['queue_name'],
            connector=value['connector'],
            encoder=value['encoder'],
        )
