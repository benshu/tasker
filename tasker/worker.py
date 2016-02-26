import multiprocessing
import multiprocessing.pool


class Worker:
    min_num_of_workers = 0
    max_num_of_workers = 4

    def __init__(self, task, concurrency):
        self.task = task
        self.concurrency = concurrency

        self.pool_context = multiprocessing.get_context('spawn')
        self.pool = multiprocessing.pool.Pool(
            processes=concurrency,
            context=self.pool_context,
        )

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

        for i in range(self.concurrency):
            async_result = self.pool.apply_async(
                func=self.task.work_loop,
            )
            async_results.append(async_result)

        while True:
            for async_result in async_results:
                async_result.wait(
                    timeout=0.1,
                )

                if async_result.ready():
                    async_results.remove(async_result)
                    async_result = self.pool.apply_async(
                        func=self.task.work_loop,
                    )
                    async_results.append(async_result)
