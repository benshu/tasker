import multiprocessing
import multiprocessing.pool
import time
import zmq


test_queue_size = 100000
zmq_port = 1234
zmq_queue_port_push = 1235
zmq_queue_port_pull = 1236


def zmq_streamer():
    try:
        context = zmq.Context()
        # Socket facing clients
        frontend = context.socket(zmq.PUSH)
        frontend.bind("tcp://*:%s" % (zmq_queue_port_push))
        # Socket facing services
        backend = context.socket(zmq.PULL)
        backend.bind("tcp://*:%s" % (zmq_queue_port_pull))

        zmq.device(zmq.STREAMER, frontend, backend)
    except Exception as e:
        print(e)
        print("bringing down zmq device")
    finally:
        pass
        frontend.close()
        backend.close()
        context.term()


def consume_queue(queue):
    queue.get()
    start = time.time()
    for i in range(test_queue_size - 1):
        queue.get()
    end = time.time()

    print('queue:')
    print(end-start)


def consume_zmq_pair():
    context = zmq.Context()
    socket = context.socket(zmq.PAIR)
    socket.bind("tcp://*:%s" % zmq_port)

    socket.recv()
    start = time.time()
    for i in range(test_queue_size - 1):
        socket.recv()
    end = time.time()

    print('zmq pair:')
    print(end-start)


def consume_zmq_streamer():
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect("tcp://localhost:%s" % zmq_queue_port_push)

    socket.recv()
    start = time.time()
    for i in range(test_queue_size - 1):
        socket.recv()
    end = time.time()

    print('zmq streamer:')
    print(end-start)


def main():
    process_pool_context = multiprocessing.get_context('spawn')
    pool = multiprocessing.pool.Pool(
        processes=4,
        context=process_pool_context,
    )
    pool.apply_async(
        func=zmq_streamer,
    )


    multiprocessing_manager = multiprocessing.Manager()
    multiprocessing_queue = multiprocessing_manager.Queue(
        maxsize=test_queue_size,
    )
    for i in range(test_queue_size):
        multiprocessing_queue.put(b'1')

    res = pool.apply_async(
        func=consume_queue,
        args=(multiprocessing_queue,),
    )
    res.get()


    context = zmq.Context()
    socket = context.socket(zmq.PAIR)
    res = pool.apply_async(
        func=consume_zmq_pair,
    )
    time.sleep(1)
    socket.connect("tcp://localhost:%s" % zmq_port)
    for i in range(test_queue_size):
        socket.send(b'1')
    res.get()
    socket.close()


    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    res = pool.apply_async(
        func=consume_zmq_streamer,
    )
    time.sleep(1)
    socket.connect("tcp://localhost:%s" % zmq_queue_port_pull)
    for i in range(test_queue_size):
        socket.send(b'1')
    res.wait()
    socket.close()

if __name__ == '__main__':
    main()
