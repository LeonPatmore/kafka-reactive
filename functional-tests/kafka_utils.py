import logging
import time

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient, TopicPartition
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

from utils import uuid, do_until_true_with_timeout, convert_to_ordered_dict

log = logging.getLogger(__name__)


class OffsetDifference(object):

    def __init__(self, first_offsets: dict[int, int], second_offsets: dict[int, int]):
        self.first_offsets = first_offsets
        self.second_offsets = second_offsets
        self.offset_difference = convert_to_ordered_dict(self._calculate_offset_difference())

    def _calculate_offset_difference(self) -> dict[int, tuple]:
        offset_difference = {}
        for partition, first_offset in self.first_offsets.items():
            second_offset = self.second_offsets.get(partition, -1)
            offset_difference[partition] = (first_offset, second_offset, (second_offset-first_offset))
        return offset_difference

    def is_up_to_date(self):
        for values_and_difference in self.offset_difference.values():
            if values_and_difference[2] > 0:
                return False
        return True

    def __str__(self) -> str:
        string = ""
        for partition, values_and_difference in self.offset_difference.items():
            string += f"[ Part {partition}, {values_and_difference[0]} - {values_and_difference[1]} " \
                      f"= {values_and_difference[2]} ], "
        return string


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
                                     consumer_timeout_ms=5000,
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

    def _get_topic_partitions(self) -> list[TopicPartition]:
        return [TopicPartition(self.topic, partition) for partition in self.consumer.partitions_for_topic(self.topic)]

    def get_latest_offsets(self) -> dict[int, int]:
        return convert_to_ordered_dict({topic_partition.partition: offset for (topic_partition, offset)
                                        in self.consumer.end_offsets(self._get_topic_partitions()).items()})

    def get_latest_offset_for_partition(self, partition: int) -> int:
        latest_offsets = self.get_latest_offsets()
        return latest_offsets.get(partition, -1)

    def get_offsets(self) -> dict[int, int]:
        return convert_to_ordered_dict({topic_partition.partition: offset_meta.offset for (topic_partition, offset_meta)
                                        in self.admin_client.list_consumer_group_offsets(self.group_id).items()})

    def get_offset_difference(self) -> OffsetDifference:
        return OffsetDifference(self.get_offsets(), self.get_latest_offsets())

    def wait_for_offset_catchup(self, timeout_seconds: int = 60):
        end_time = time.time() + timeout_seconds
        while time.time() < end_time:
            try:
                self.assert_group_up_to_date()
                return
            except Exception as e:
                log.info(e)
            time.sleep(1)
        raise Exception("Timed out!")

    def assert_group_up_to_date(self):
        assert self.get_offset_difference().is_up_to_date()

    def ensure_not_up_to_date_for_n_seconds(self, seconds: int):
        end_time = time.time() + seconds
        while time.time() < end_time:
            offset_difference = self.get_offset_difference()
            log.info("Offset difference: " + str(offset_difference))
            if offset_difference.is_up_to_date():
                raise Exception("Offsets are up to date!")
            time.sleep(2)
