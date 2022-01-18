from src.core.utils.channel import Channel
import re
import sys
import numpy as np

class CrashChannel(Channel):
    def __init__(self, regex_pattern : str, prob : float):
        super().__init__()

        self._p = re.compile(regex_pattern)
        self._prob = prob

    def consume(self, topic=""):
        self._semaphores[topic].acquire()
        with self._locks[topic]:
            msg = self._topic_queues[topic].pop(0)
        
        if self._p.match(msg):
            v = np.random.random_sample()
            if v <= self._prob:
                print("exit on", msg)
                sys.exit(1)

        return msg