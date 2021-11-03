import logging
import time

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient, TopicPartition
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

from utils import uuid, do_until_true_with_timeout

log = logging.getLogger(__name__)


class KafkaUtils(object):

    def __init__(self, bootstrap_servers: list, topic: str, group_id: str):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                      api_version=(5, 5, 1),
                                      request_timeout_ms=1000)
        self.consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
        self.admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id

    def has_consumer_group(self) -> bool:
        for group in self.admin_client.list_consumer_groups():
            if group[0] == self.group_id:
                return True
        return False

    def wait_until_consumer_group(self):
        do_until_true_with_timeout(self.has_consumer_group)

    def consume_messages_and_close(self):
        tmp_consumer = KafkaConsumer(self.topic,
                                     bootstrap_servers=self.bootstrap_servers,
                                     auto_offset_reset='earliest',
                                     group_id=self.group_id,
                                     consumer_timeout_ms=2000,
                                     enable_auto_commit=True)
        for msg in tmp_consumer:
            log.info(f"Found message [ {msg.value} ]")
        tmp_consumer.close()

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

    def produce_element_with_delay(self, delay_ms: int):
        key = uuid()
        log.info(f"Producing element with key [ {key} ] and delay [ {delay_ms} ]")
        self._produce_record_sync(key, str(delay_ms))

    def _get_topic_partitions(self) -> list:
        return [TopicPartition(self.topic, partition) for partition in self.consumer.partitions_for_topic(self.topic)]

    def get_latest_offsets(self) -> dict:
        return self.consumer.end_offsets(self._get_topic_partitions())

    def get_latest_offset_for_partition(self, partition: TopicPartition) -> int:
        latest_offsets = self.get_latest_offsets()
        for latest_partition, latest_offset in latest_offsets.items():
            if partition.partition == latest_partition.partition:
                return latest_offset

    def get_offsets(self) -> dict:
        return self.admin_client.list_consumer_group_offsets(self.group_id)

    def wait_for_offset_catchup(self, timeout_seconds: int = 60):
        end_time = time.time() + timeout_seconds
        while time.time() < end_time:
            try:
                self.ensure_group_up_to_date()
                return
            except Exception as e:
                log.info(e)
            time.sleep(1)
        raise Exception("Timed out!")

    def ensure_group_up_to_date(self):
        current_offsets = self.get_offsets()

        for partition, current_offset in current_offsets.items():
            latest_offset = self.get_latest_offset_for_partition(partition)
            if current_offset.offset < latest_offset:
                raise Exception(f"Not up to date for partition [ {partition} ]."
                                f" Current offset is {current_offset.offset}, latest is [ {latest_offset} ]")

        log.info(f"Current offsets [ {current_offsets} ] are up to date!")
