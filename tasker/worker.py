import logging
import multiprocessing
import multiprocessing.pool


class Worker:
    min_num_of_workers = 0
    max_num_of_workers = 4

    def __init__(self, task, concurrency):
        self.logger = self._create_logger()

        self.task = task
        self.concurrency = concurrency

        self.pool_context = multiprocessing.get_context('spawn')
        self.pool = multiprocessing.pool.Pool(
            processes=concurrency,
            context=self.pool_context,
        )

        self.logger.debug('initialized')

    def _create_logger(self):
        '''
        '''
        logger = logging.getLogger(
            name='Worker',
        )

        for handler in logger.handlers:
            logger.removeHandler(handler)

        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt='%(asctime)s %(name)-12s %(levelname)-8s %(funcName)-16s -> %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
        )
        handler.setFormatter(formatter)

        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        return logger

    def add_worker(self):
        '''
        '''
        pass

    def remove_worker(self):
        '''
        '''
        pass

    def start(self):
        '''
        '''
        async_results = []

        self.logger.debug('started')

        for i in range(self.concurrency):
            async_result = self.pool.apply_async(
                func=self.task.work_loop,
            )
            async_results.append(async_result)

            self.logger.debug('task applied async')

        while True:
            for async_result in async_results:
                async_result.wait(
                    timeout=0.1,
                )

                if async_result.ready():
                    try:
                        async_result.get()
                    except Exception as exc:
                        self.logger.error(
                            'task execution raised an exception: {exc}'.format(
                                exc=exc,
                            )
                        )

                    async_results.remove(async_result)

                    self.logger.debug('task removed')

                    async_result = self.pool.apply_async(
                        func=self.task.work_loop,
                    )
                    async_results.append(async_result)

                    self.logger.debug('task reapplied')
