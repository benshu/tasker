import queue
import multiprocessing
import multiprocessing.pool
import time


test_queue_size = 100000


def consume_queue(queue):
    queue.get()
    start = time.time()
    for i in range(test_queue_size - 1):
        queue.get()
    end = time.time()

    print('queue:')
    print(end-start)


def main():
    process_pool_context = multiprocessing.get_context('spawn')
    pool = multiprocessing.pool.Pool(
        processes=2,
        context=process_pool_context,
    )

    multiprocessing_manager = multiprocessing.Manager()
    multiprocessing_queue = multiprocessing_manager.Queue(
        maxsize=test_queue_size,
    )

    start = time.time()
    for i in range(test_queue_size):
        multiprocessing_queue.put(b'1')
    end = time.time()

    print('queue INSERTION:')
    print(end-start)

    pool.apply(func=consume_queue, args=(multiprocessing_queue,), kwds={})

    regular_queue = queue.Queue()
    start = time.time()
    for i in range(test_queue_size):
        regular_queue.put(b'1')
    end = time.time()

    print('queue INSERTION:')
    print(end-start)
    consume_queue(regular_queue)

if __name__ == '__main__':
    main()
