import threading


class Channel:
    def __init__(self):
        self._semaphores = {"": threading.Semaphore(0)}
        self._locks = {"": threading.Lock()}
        self._config_lock = threading.Lock()
        self._topic_queues = {"": []}
        self._trash = False

    def create_topic(self, topic):
        with self._config_lock:
            if topic not in self._topic_queues:
                self._topic_queues[topic] = []
                self._locks[topic] = threading.Lock()
                self._semaphores[topic] = threading.Semaphore(0)

    def produce(self, msg, topic="", trash=False):
        if not self._trash:
            if topic != "" and topic not in self._topic_queues:
                self.create_topic(topic)
            with self._locks[topic]:
                self._topic_queues[topic].append((msg, trash))
                self._semaphores[topic].release()

    def consume(self, topic=""):
        self._semaphores[topic].acquire()
        with self._locks[topic]:
            msg = self._topic_queues[topic].pop(0)[0]
        return msg

    def is_empty(self, topic=""):
        with self._locks[topic]:
            size = len(self._topic_queues[topic])
        return size == 0

    def set_trash_flag(self, flag : bool):
        self._trash = flag


