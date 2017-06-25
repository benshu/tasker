import time

from .. import logger


class Storage:
    name = 'Storage'

    def __init__(
        self,
        connector,
        encoder,
    ):
        self.logger = logger.logger.Logger(
            logger_name=self.name,
        )

        self.connector = connector
        self.encoder = encoder

    def acquire_lock_key(
        self,
        name,
        ttl=None,
        timeout=None,
    ):
        try:
            sleep_interval = 0.1
            lock_key_name = '_storage_{key_name}_lock'.format(
                key_name=name,
            )

            if timeout is not None:
                run_forever = False
                time_remaining = timeout
            else:
                run_forever = True

            while run_forever or time_remaining > 0:
                locked = self.set_key(
                    name=lock_key_name,
                    value='locked',
                    ttl=ttl,
                )
                if locked:
                    return True

                time.sleep(sleep_interval)

                if not run_forever:
                    time_remaining -= sleep_interval

            return False
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def release_lock_key(
        self,
        name,
    ):
        try:
            self.connector.key_del(
                keys=[
                    name,
                    '_storage_{key_name}_lock'.format(
                        key_name=name,
                    ),
                ],
            )
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def get_key(
        self,
        name,
    ):
        try:
            value = self.connector.key_get(
                key=name,
            )
            if not value:
                return {}

            decoded_value = self.encoder.decode(
                data=value,
            )

            return decoded_value
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def set_key(
        self,
        name,
        value,
        ttl=None,
    ):
        try:
            encoded_value = self.encoder.encode(
                data=value,
            )

            key_was_set = self.connector.key_set(
                key=name,
                value=encoded_value,
                ttl=ttl,
            )

            return key_was_set
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            raise exception

    def __getstate__(
        self,
    ):
        state = {
            'connector': self.connector,
            'encoder': self.encoder,
        }

        return state

    def __setstate__(
        self,
        state,
    ):
        self.__init__(
            connector=state['connector'],
            encoder=state['encoder'],
        )
