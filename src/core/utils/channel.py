import threading


class Channel:
    def __init__(self):
        self._semaphore = threading.Semaphore(0)
        self._lock = threading.Lock()
        self._queue = []

    def produce(self, msg):
        with self._lock:
            self._queue.append(msg)
            self._semaphore.release()

    def consume(self):
        self._semaphore.acquire()
        with self._lock:
            msg = self._queue.pop(0)
        return msg
