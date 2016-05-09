import time
import zmq
import multiprocessing


def consumer_1():
    context = zmq.Context()
    consumer_receiver = context.socket(zmq.REQ)
    consumer_receiver.connect("tcp://127.0.0.1:5557")

    print(time.time())
    for i in range(50000):
        consumer_receiver.send(b'')
        consumer_receiver.recv()
    print(time.time())


def consumer_2():
    context = zmq.Context()
    consumer_receiver = context.socket(zmq.REQ)
    consumer_receiver.connect("tcp://127.0.0.1:5557")

    print(time.time())
    for i in range(50000):
        consumer_receiver.send(b'')
        consumer_receiver.recv()
    print(time.time())


def producer():
    context = zmq.Context()
    zmq_socket = context.socket(zmq.REP)
    zmq_socket.bind("tcp://127.0.0.1:5557")

    for i in range(100000):
        zmq_socket.recv()
        zmq_socket.send(b'1'*256)



if __name__ == "__main__":
    multiprocessing.Process(target=producer, args=()).start()
    time.sleep(1)
    multiprocessing.Process(target=consumer_1, args=()).start()
    time.sleep(1)
    multiprocessing.Process(target=consumer_2, args=()).start()
    time.sleep(10)
