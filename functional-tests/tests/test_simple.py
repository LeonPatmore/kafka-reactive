import logging
from time import sleep

import pytest

from cofiguration import kafka_utils
from kafka_processor_service import BatchConsumerFactory, SpringReactorFactory, KafkaParallelConsumerFactory
from service_starter import ServiceInstance
from utils import do_for_n_seconds

logging.getLogger("kafka.conn").setLevel(logging.WARN)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


@pytest.fixture(scope="session", autouse=True)
def ensure_topic():
    kafka_utils.ensure_topic_created()


@pytest.fixture(params=[KafkaParallelConsumerFactory()], scope="session")
def given_factory(request):
    return request.param


@pytest.fixture
def given_service(request, given_factory) -> ServiceInstance:
    instance = given_factory.generate_instance()
    request.addfinalizer(lambda: instance.stop())
    return instance


@pytest.fixture
def given_kafka_up_to_date():
    kafka_utils.consume_messages_and_close()
    kafka_utils.wait_for_offset_catchup()


def test_simple(given_service, given_kafka_up_to_date):
    log.info("Starting offset: " + str(kafka_utils.get_offsets()))
    log.info("Latest offsets: " + str(kafka_utils.get_latest_offsets()))

    kafka_utils.produce_element_with_delay(1000)

    log.info("New offset: " + str(kafka_utils.get_offsets()))
    log.info("New latest offsets: " + str(kafka_utils.get_latest_offsets()))

    # given_service.start()

    kafka_utils.wait_for_offset_catchup()


def test_when_instance_dies(request, given_factory, given_kafka_up_to_date):
    log.info("Starting offset: " + str(kafka_utils.get_offsets()))
    log.info("Latest offsets: " + str(kafka_utils.get_latest_offsets()))

    kafka_utils.produce_element_with_delay(13000)

    sleep(5)

    log.info("New offset: " + str(kafka_utils.get_offsets()))
    log.info("New latest offsets: " + str(kafka_utils.get_latest_offsets()))

    instance1 = given_factory.generate_instance()
    instance1.start()
    kafka_utils.wait_until_consumer_group()
    do_for_n_seconds(lambda: log.info("Latest offsets: " + str(kafka_utils.get_offsets())), 15)
    instance1.stop()

    sleep(1)

    instance2 = given_factory.generate_instance()
    request.addfinalizer(lambda: instance2.stop())
    instance2.start()

    kafka_utils.wait_for_offset_catchup()


def test_when_batch_with_delay_blocks_next_batch(request, given_service, given_kafka_up_to_date):
    log.info("Starting offset: " + str(kafka_utils.get_offsets()))
    log.info("Latest offsets: " + str(kafka_utils.get_latest_offsets()))

    for _ in range(5):
        kafka_utils.produce_element_with_delay(60000)

    log.info("Current offsets: " + str(kafka_utils.get_offsets()))

    # given_service.start()
    kafka_utils.wait_until_consumer_group()
    sleep(15)

    log.info("Current offsets: " + str(kafka_utils.get_offsets()))
    kafka_utils.produce_element_with_delay(1000)

    kafka_utils.ensure_not_up_to_date_for_n_seconds(30)

    do_for_n_seconds(lambda: log.info("Offsets: " + str(kafka_utils.get_offset_difference())), 500)
    kafka_utils.wait_for_offset_catchup()


def test_generate_records():
    log.info("Current offsets: " + str(kafka_utils.get_offsets()))
    for _ in range(5):
        kafka_utils.produce_element_with_delay(1200000)
    do_for_n_seconds(lambda: log.info("Offsets: " + str(kafka_utils.get_offset_difference())), 500)


def test_generate_error_element():
    log.info("Current offsets: " + str(kafka_utils.get_offsets()))
    kafka_utils.produce_element_with_delay(-1)
    do_for_n_seconds(lambda: log.info("Offsets: " + str(kafka_utils.get_offset_difference())), 500)
