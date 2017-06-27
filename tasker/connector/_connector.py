from .. import logger


class Connector:
    name = 'Connector'

    def __init__(
        self,
    ):
        self.logger = logger.logger.Logger(
            logger_name='Connector',
        )

    def key_set(
        self,
        key,
        value,
        ttl=None,
    ):
        raise NotImplementedError()

    def key_get(
        self,
        key,
    ):
        raise NotImplementedError()

    def key_del(
        self,
        keys,
    ):
        raise NotImplementedError()

    def pop(
        self,
        key,
        timeout=0,
    ):
        raise NotImplementedError()

    def pop_bulk(
        self,
        key,
        count,
    ):
        raise NotImplementedError()

    def push(
        self,
        key,
        value,
    ):
        raise NotImplementedError()

    def push_bulk(
        self,
        key,
        values,
    ):
        raise NotImplementedError()

    def add_to_set(
        self,
        set_name,
        value,
    ):
        raise NotImplementedError()

    def remove_from_set(
        self,
        set_name,
        value,
    ):
        raise NotImplementedError()

    def is_member_of_set(
        self,
        set_name,
        value,
    ):
        raise NotImplementedError()

    def len(
        self,
        key,
    ):
        raise NotImplementedError()

    def delete(
        self,
        key,
    ):
        raise NotImplementedError()

    def __getstate__(
        self,
    ):
        raise NotImplementedError()

    def __setstate__(
        self,
        value,
    ):
        raise NotImplementedError()
