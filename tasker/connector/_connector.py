import logging

from .. import logger


class Connector:
    '''
    '''
    name = 'Connector'

    def __init__(self):
        self.logger = logger.logger.Logger(
            logger_name=self.name,
            log_level=logging.ERROR,
        )

    def pop(self, key, timeout=0):
        '''
        '''
        try:
            return self._pop(
                key=key,
                timeout=timeout,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _pop(self, key, timeout=0):
        '''
        '''
        raise NotImplemented()

    def pop_bulk(self, key, count):
        '''
        '''
        try:
            return self._pop_bulk(
                key=key,
                count=count,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _pop_bulk(self, key, count):
        '''
        '''
        raise NotImplemented()

    def push(self, key, value):
        '''
        '''
        try:
            return self._push(
                key=key,
                value=value,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _push(self, key, value):
        '''
        '''
        raise NotImplemented()

    def push_bulk(self, key, values):
        '''
        '''
        try:
            return self._push_bulk(
                key=key,
                values=values,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _push_bulk(self, key, values):
        '''
        '''
        raise NotImplemented()

    def add_to_set(self, set_name, value):
        '''
        '''
        try:
            return self._add_to_set(
                set_name=set_name,
                value=value,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _add_to_set(self, set_name, value):
        '''
        '''
        raise NotImplemented()

    def remove_from_set(self, set_name, value):
        '''
        '''
        try:
            return self._remove_from_set(
                set_name=set_name,
                value=value,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _remove_from_set(self, set_name, value):
        '''
        '''
        raise NotImplemented()

    def is_member_of_set(self, set_name, value):
        '''
        '''
        try:
            return self._is_member_of_set(
                set_name=set_name,
                value=value,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _is_member_of_set(self, set_name, value):
        '''
        '''
        raise NotImplemented()

    def len(self, key):
        '''
        '''
        try:
            return self._len(
                key=key,
                )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _len(self, key):
        '''
        '''
        raise NotImplemented()

    def delete(self, key):
        '''
        '''
        try:
            return self._delete(
                key=key,
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def _delete(self, key):
        '''
        '''
        raise NotImplemented()

    def __getstate__(self):
        '''
        '''
        raise NotImplemented()

    def __setstate__(self, value):
        '''
        '''
        raise NotImplemented()
