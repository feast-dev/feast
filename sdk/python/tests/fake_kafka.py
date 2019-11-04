import queue
from typing import Dict


class FakeKafka:
    def __init__(self):
        self._messages = dict()  # type: Dict[str, queue.Queue]

    def send(self, topic, message):
        if topic not in self._messages:
            self._messages[topic] = queue.Queue()
        self._messages[topic].queue.append(message)

    def get(self, topic: str):
        message = None
        if self._messages[topic]:
            message = self._messages[topic].get(block=False)
        return message

    def flush(self, timeout):
        return True
