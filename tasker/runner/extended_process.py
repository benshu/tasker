import logging
import multiprocessing
import traceback

from .. import logger


class Process(multiprocessing.Process):
    '''
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.logger = logger.logger.Logger(
            logger_name='ExtendedProcess',
            log_level=logging.ERROR,
        )

        multiprocessing.set_start_method(
            method='spawn',
            force=True,
        )

        self._pipe = multiprocessing.Pipe()

        self._parent_connection = self._pipe[0]
        self._child_connection = self._pipe[1]

        self._exception = None

    def run(self):
        try:
            super().run()

            self._child_connection.send(None)
        except Exception as exception:
            self._child_connection.send(
                {
                    'exception': exception,
                    'traceback': traceback.format_exc(),
                }
            )

    @property
    def exception(self):
        try:
            if self._exception is not None:
                return self._exception

            if self._parent_connection.poll():
                self._exception = self._parent_connection.recv()

            return self._exception
        except Exception as exception:
            self.logger.error(
                msg=exception,
            )

            return None
