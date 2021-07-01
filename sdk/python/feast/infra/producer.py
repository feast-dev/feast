import random
import string

from kinesis.producer import KinesisProducer


class Producer(object):
    def __init__(self):
        self.producer = KinesisProducer("feast-stream-testing")

    @staticmethod
    def generate_data(i):
        return "".join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(i % 20)
        )

    def write(self, n):
        for i in range(n):
            record = self.generate_data(i)
            self.producer.put(record)


if __name__ == "__main__":
    p = Producer()
    p.write(10)
