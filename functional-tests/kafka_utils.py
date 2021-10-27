import logging

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError


class KafkaUtils(object):

    def __init__(self, bootstrap_servers: list, topic: str):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                      api_version=(5, 5, 1),
                                      request_timeout_ms=1000)
        self.consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
        self.admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        self.topic = topic

    def ensure_topic_created(self):
        try:
            self.admin_client.create_topics([NewTopic(self.topic, 2, 1)])
        except TopicAlreadyExistsError:
            pass

    def _produce_record_sync(self, key: str, value: str):
        future = self.producer.send(self.topic, str.encode(value), str.encode(key))
        try:
            future.get(5)
            self.producer.flush(5)
        except KafkaError as e:
            logging.warning("Could not produce Kafka record!" + str(e))
            raise e

    def produce_element(self):
        self._produce_record_sync("key1", "value1")

    def get_offsets(self, group_id: str) -> dict:
        return self.admin_client.list_consumer_group_offsets(group_id)
