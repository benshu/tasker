import multiprocessing
import queue

from . import _queue


class Queue(_queue.Queue):
    '''
    '''
    name = 'shared'

    def __init__(self, tasks_per_transaction, *args, **kwargs):
        '''
        '''
        super().__init__(*args, **kwargs)

        self.tasks_per_transaction = tasks_per_transaction

        multiprocessing_manager = multiprocessing.Manager()
        self.multiprocessing_queue = multiprocessing_manager.Queue()

    def fill_queue(self):
        '''
        '''
        values = self.connector.pop_bulk(
            key=self.queue_name,
            count=self.tasks_per_transaction,
        )

        for value in values:
            self.multiprocessing_queue.put(
                item=value,
            )

    def _dequeue(self, timeout):
        '''
        '''
        if self.multiprocessing_queue.empty():
            self.fill_queue()

        try:
            value = self.multiprocessing_queue.get(
                block=True,
                timeout=1,
            )

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
            'queue_name': self.queue_name,
            'connector': self.connector,
            'encoder': self.encoder,
            'multiprocessing_queue': self.multiprocessing_queue,
            'logger': self.logger,
        }

        return state

    def __setstate__(self, value):
        '''
        '''
        self.tasks_per_transaction = value['tasks_per_transaction']
        self.queue_name = value['queue_name']
        self.connector = value['connector']
        self.encoder = value['encoder']
        self.multiprocessing_queue = value['multiprocessing_queue']
        self.logger = value['logger']
