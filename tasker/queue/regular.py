from . import _queue


class Queue(
    _queue.Queue,
):
    name = 'regular'

    def _dequeue(
        self,
        queue_name,
    ):
        value = self.connector.pop(
            key=queue_name,
        )

        if value is None:
            return None

        return value

    def _dequeue_bulk(
        self,
        queue_name,
        count,
    ):
        values = self.connector.pop_bulk(
            key=queue_name,
            count=count,
        )

        return values

    def _enqueue(
        self,
        queue_name,
        value,
    ):
        pushed = self.connector.push(
            key=queue_name,
            value=value,
        )

        return pushed

    def _enqueue_bulk(
        self,
        queue_name,
        values,
    ):
        pushed = self.connector.push_bulk(
            key=queue_name,
            values=values,
        )

        return pushed

    def _add_result(
        self,
        queue_name,
        value,
    ):
        added = self.connector.add_to_set(
            set_name='{queue_name}.results'.format(
                queue_name=queue_name,
            ),
            value=value,
        )

        return added

    def _remove_result(
        self,
        queue_name,
        value,
    ):
        removed = self.connector.remove_from_set(
            set_name='{queue_name}.results'.format(
                queue_name=queue_name,
            ),
            value=value,
        )

        return removed

    def _has_result(
        self,
        queue_name,
        value,
    ):
        is_in_set = self.connector.is_member_of_set(
            set_name='{queue_name}.results'.format(
                queue_name=queue_name,
            ),
            value=value,
        )

        return is_in_set

    def _len(
        self,
        queue_name,
    ):
        queue_len = self.connector.len(
            key=queue_name,
        )

        return queue_len

    def _flush(
        self,
        queue_name,
    ):
        self.connector.delete(
            key=queue_name,
        )
        self.connector.delete(
            key='{queue_name}.results'.format(
                queue_name=queue_name,
            ),
        )
