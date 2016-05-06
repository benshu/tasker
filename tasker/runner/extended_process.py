import multiprocessing
import traceback


class Process(multiprocessing.Process):
    '''
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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
        if self._exception is not None:
            return self._exception

        if self._parent_connection.poll():
            self._exception = self._parent_connection.recv()

        return self._exception
