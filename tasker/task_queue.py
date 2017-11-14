import time
import datetime
import random

from . import logger


class TaskQueue:
    def __init__(
        self,
        queue,
    ):
        self.queue = queue
        self.logger = logger.logger.Logger(
            logger_name='TaskQueue',
        )

    def purge_tasks(
        self,
        task_name,
    ):
        try:
            self.queue.flush(
                queue_name=task_name,
            )
        except Exception as exception:
            self.logger.error(
                msg='could not purge tasks: {exception}'.format(
                    exception=exception,
                )
            )

    def number_of_enqueued_tasks(
        self,
        task_name,
    ):
        try:
            number_of_enqueued_tasks = self.queue.len(
                queue_name=task_name,
            )

            return number_of_enqueued_tasks
        except Exception as exception:
            self.logger.error(
                msg='could not get len of queue: {exception}'.format(
                    exception=exception,
                )
            )

    def craft_task(
        self,
        task_name,
        args=(),
        kwargs={},
        report_completion=False,
    ):
        if report_completion:
            completion_key = self.create_completion_key(
                task_name=task_name,
            )
        else:
            completion_key = None

        task = {
            'name': task_name,
            'date': datetime.datetime.utcnow().timestamp(),
            'args': args,
            'kwargs': kwargs,
            'run_count': 0,
            'completion_key': completion_key,
        }

        return task

    def create_completion_key(
        self,
        task_name,
    ):
        added = False

        while not added:
            completion_key = random.randint(0, 9999999999999)
            added = self.queue.add_result(
                queue_name=task_name,
                value=completion_key,
            )

        return completion_key

    def report_complete(
        self,
        task,
    ):
        completion_key = task['completion_key']

        if completion_key:
            removed = self.queue.remove_result(
                queue_name=task['name'],
                value=completion_key,
            )

            return removed
        else:
            return True

    def wait_task_finished(
        self,
        task,
        timeout=0,
    ):
        completion_key = task['completion_key']
        remaining_time = timeout

        if not completion_key:
            return

        has_result = self.queue.has_result(
            queue_name=task['name'],
            value=completion_key,
        )
        while has_result:
            if timeout and remaining_time <= 0:
                return

            has_result = self.queue.has_result(
                queue_name=task['name'],
                value=completion_key,
            )

            time.sleep(0.5)
            remaining_time -= 0.5

    def wait_queue_empty(
        self,
        task_name,
        timeout=0,
    ):
        remaining_time = timeout

        not_empty = True
        while not_empty:
            if timeout and remaining_time <= 0:
                return

            not_empty = self.number_of_enqueued_tasks(
                task_name=task_name,
            ) != 0

            time.sleep(1.0)
            remaining_time -= 1.0

    def apply_async_one(
        self,
        task,
        time_to_enqueue=None,
    ):
        try:
            self.queue.enqueue(
                queue_name=task['name'],
                value=task,
                time_to_enqueue=time_to_enqueue,
            )

            return True
        except Exception as exception:
            self.logger.error(
                msg='could not push task: {exception}'.format(
                    exception=exception,
                )
            )

            return False

    def apply_async_many(
        self,
        tasks,
    ):
        if len(tasks) == 0:
            return True

        task_name_to_tasks = {}
        for task in tasks:
            if task['name'] in task_name_to_tasks:
                task_name_to_tasks[task['name']] += [task]
            else:
                task_name_to_tasks[task['name']] = [task]

        try:
            for task_name, tasks in task_name_to_tasks.items():
                self.queue.enqueue_bulk(
                    queue_name=task_name,
                    values=tasks,
                )

            return True
        except Exception as exception:
            self.logger.error(
                msg='could not push tasks: {exception}'.format(
                    exception=exception,
                )
            )

            return False

    def get_tasks(
        self,
        task_name,
        number_of_tasks,
    ):
        try:
            if number_of_tasks == 1:
                task = self.queue.dequeue(
                    queue_name=task_name,
                )

                if task:
                    return [task]
                else:
                    return []
            else:
                tasks = self.queue.dequeue_bulk(
                    queue_name=task_name,
                    count=number_of_tasks,
                )

                return tasks
        except Exception as exception:
            self.logger.error(
                msg='could not pull task: {exception}'.format(
                    exception=exception,
                )
            )

            return []

    def retry(
        self,
        task,
    ):
        task['run_count'] += 1

        return self.apply_async_one(
            task=task,
        )

    def requeue(
        self,
        task,
    ):
        return self.apply_async_one(
            task=task,
        )
