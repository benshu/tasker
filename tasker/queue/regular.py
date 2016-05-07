from . import _queue


class Queue(_queue.Queue):
    '''
    '''
    name = 'regular'

    def _dequeue(self, timeout=0):
        '''
        '''
        value = self.connector.pop(
            key=self.queue_name,
            timeout=timeout,
        )

        if value is None:
            return None

        return value

    def _dequeue_bulk(self, count):
        '''
        '''
        values = self.connector.pop_bulk(
            key=self.queue_name,
            count=count,
        )

        return values

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
