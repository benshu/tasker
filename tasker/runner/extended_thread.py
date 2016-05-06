import threading
import multiprocessing
import traceback


class Thread(threading.Thread):
    '''
    '''
    _exc_pipe = multiprocessing.Pipe()
    _ret_pipe = multiprocessing.Pipe()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._exc_parent_connection = self._exc_pipe[0]
        self._exc_child_connection = self._exc_pipe[1]

        self._ret_parent_connection = self._ret_pipe[0]
        self._ret_child_connection = self._ret_pipe[1]

        self._exception = None
        self._returned_value = None

    def run(self):
        try:
            if self._target:
                returned_value = self._target(*self._args, **self._kwargs)

                if returned_value:
                    self._ret_child_connection.send(returned_value)
        except Exception as exception:
            self._exc_child_connection.send(
                {
                    'exception': exception,
                    'traceback': traceback.format_exc(),
                }
            )
        finally:
            del self._target
            del self._args
            del self._kwargs

    @property
    def returned_value(self):
        if self._returned_value is not None:
            return self._returned_value

        if self._ret_parent_connection.poll():
            self._returned_value = self._ret_parent_connection.recv()

        return self._returned_value

    @property
    def exception(self):
        if self._exception is not None:
            return self._exception

        if self._exc_parent_connection.poll():
            self._exception = self._exc_parent_connection.recv()

        return self._exception
