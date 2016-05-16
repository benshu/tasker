import tempfile
import os
import logging


class Logger:
    '''
    '''
    log_level = logging.ERROR
    logs_dir_path = os.path.join(
        tempfile.gettempdir(),
        'tasker',
    )

    def __init__(self, logger_name):
        self.logger_name = logger_name

        logs_dir_path = os.path.join(
            self.logs_dir_path,
            logger_name,
        )
        self.create_logs_dir_path(
            logs_dir_path=logs_dir_path,
        )
        self.log_file_path = '{logs_dir_path}/{logger_name}.log'.format(
            logs_dir_path=logs_dir_path,
            logger_name=logger_name,
        )

        self.logger = logging.getLogger(
            name=self.logger_name,
        )

        for handler in self.logger.handlers:
            self.logger.removeHandler(handler)

        stream_handler = logging.StreamHandler()
        file_handler = logging.FileHandler(
            filename=self.log_file_path,
            mode='a',
            encoding='utf-8',
        )

        formatter = logging.Formatter(
            fmt='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )

        stream_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        self.logger.addHandler(stream_handler)
        self.logger.addHandler(file_handler)

        self.logger.setLevel(self.log_level)

    def create_logs_dir_path(self, logs_dir_path):
        '''
        '''
        try:
            os.makedirs(logs_dir_path)
        except:
            pass

    def debug(self, msg):
        '''
        '''
        self.logger.debug(
            msg=msg,
        )

    def warning(self, msg):
        '''
        '''
        self.logger.warning(
            msg=msg,
        )

    def info(self, msg):
        '''
        '''
        self.logger.info(
            msg=msg,
        )

    def error(self, msg):
        '''
        '''
        self.logger.error(
            msg=msg,
        )

    def log_task_failure(
        self,
        failure_reason,
        task_name,
        args,
        kwargs,
        exception,
        exception_traceback,
    ):
        '''
        '''
        traceback_formatted = exception_traceback.split('\n')
        traceback_formatted = [
            '\t\t\t | ' + line
            for line in traceback_formatted
        ]
        traceback_formatted = '\n'.join(traceback_formatted)
        traceback_formatted = '\n' + traceback_formatted

        self.error(
            '''
                --------------------------------------------
                {failure_reason}:
                - task: {task_name}
                - args: {args}
                - kwargs: {kwargs}
                - exception: {exception}
                - traceback: {traceback}
                --------------------------------------------
            '''.format(
                failure_reason=failure_reason,
                task_name=task_name,
                args=args,
                kwargs=kwargs,
                exception=exception,
                traceback=traceback_formatted,
            )
        )

    def log_task_success(
        self,
        task_name,
        args,
        kwargs,
        returned_value,
    ):
        '''
        '''
        self.info(
            '''
                Success:
                - task: {task_name}
                - args: {args}
                - kwargs: {kwargs}
                - returned_value: {returned_value}
            '''.format(
                task_name=task_name,
                args=args,
                kwargs=kwargs,
                returned_value=returned_value,
            )
        )

    def log_task_retry(
        self,
        task_name,
        args,
        kwargs,
    ):
        '''
        '''
        self.logger.warning(
            '''
                Retry:
                - task: {task_name}
                - args: {args}
                - kwargs: {kwargs}
            '''.format(
                task_name=self.name,
                args=args,
                kwargs=kwargs,
            )
        )

    def __getstate__(self):
        '''
        '''
        state = {
            'logger_name': self.logger_name,
            'log_level': self.log_level,
        }

        return state

    def __setstate__(self, value):
        '''
        '''
        self.log_level = value['log_level']

        self.__init__(
            logger_name=value['logger_name'],
        )
