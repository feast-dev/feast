import ray
from kinesis.consumer import KinesisConsumer


@ray.remote
class KinesisRayConsumer(object):
    def __init__(self, _session, kinesis_options):

        self.kinesis_consumer = KinesisConsumer(
            kinesis_options.stream_name, boto3_session=_session
        )
        print(f"Created actor with session: {_session}")

    def consume(self):
        import time

        print(f"Starting consuming at {time.time()}")

        for record in self.kinesis_consumer:
            print(record)
