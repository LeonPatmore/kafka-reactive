import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError


class KafkaUtils(object):

    def __init__(self, bootstrap_servers: list, topic: str):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                      api_version=(5, 5, 1),
                                      request_timeout_ms=2000)
        self.topic = topic

    def _produce_record_sync(self, key: str, value: str):

        def error(**a):
            pass

        def done(**b):
            pass

        future = self.producer.send(self.topic, str.encode(value), str.encode(key)).add_errback(error).add_errback(done)

        future.get(5)
        self.producer.flush(5)

        # try:
        #     future.get(5)
        #     self.producer.flush(5)
        # except KafkaError as e:
        #     logging.warning("Could not produce Kafka record!" + str(e))
        #     raise e

    def produce_element(self):
        self._produce_record_sync("key1", "value1")
