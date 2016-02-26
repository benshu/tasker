import pickle
import gzip


class Queue:
    '''
    '''
    def __init__(self, connector, queue_name, compress):
        '''
        '''
        self.connector = connector
        self.queue_name = queue_name
        self.compress = compress

        self.connector.connect()

    def dequeue(self, timeout=0):
        '''
        '''
        task_obj = self.connector.pop(
            key=self.queue_name,
            timeout=timeout,
        )

        if task_obj is None:
            return None

        if self.compress:
            task_obj_data = gzip.decompress(
                data=task_obj[1],
            )
        else:
            task_obj_data = task_obj[1]

        task = pickle.loads(task_obj_data)

        return task

    def enqueue(self, task):
        '''
        '''
        task_obj = pickle.dumps(task)

        if self.compress:
            task_obj_data = gzip.compress(
                data=task_obj,
            )
        else:
            task_obj_data = task_obj

        return self.connector.push(
            key=self.queue_name,
            value=task_obj_data,
        )

    def len(self):
        '''
        '''
        return self.connector.len(
            key=self.queue_name,
        )
