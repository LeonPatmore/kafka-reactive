import logging
from time import sleep

import pytest

from cofiguration import kafka_utils
from kafka_processor_service import BatchConsumerFactory
from service_starter import ServiceInstance

logging.getLogger("kafka.conn").setLevel(logging.WARN)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


@pytest.fixture(scope="session", autouse=True)
def ensure_topic():
    kafka_utils.ensure_topic_created()


@pytest.fixture(params=[BatchConsumerFactory()], scope="session")
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
    kafka_utils.ensure_group_up_to_date()


def test_simple(given_service, given_kafka_up_to_date):
    log.info("Starting offset: " + str(kafka_utils.get_offsets()))
    log.info("Latest offsets: " + str(kafka_utils.get_latest_offsets()))

    kafka_utils.produce_element_with_delay(1000)

    log.info("New offset: " + str(kafka_utils.get_offsets()))
    log.info("New latest offsets: " + str(kafka_utils.get_latest_offsets()))

    given_service.start()

    kafka_utils.wait_for_offset_catchup()

    given_service.stop()


def test_when_instance_dies(request, given_factory, given_kafka_up_to_date):
    log.info("Starting offset: " + str(kafka_utils.get_offsets()))
    log.info("Latest offsets: " + str(kafka_utils.get_latest_offsets()))

    kafka_utils.produce_element_with_delay(8000)

    log.info("New offset: " + str(kafka_utils.get_offsets()))
    log.info("New latest offsets: " + str(kafka_utils.get_latest_offsets()))

    instance1 = given_factory.generate_instance()
    instance1.start()
    kafka_utils.wait_until_consumer_group()
    sleep(10)
    instance1.stop()

    sleep(1)

    instance2 = given_factory.generate_instance()
    request.addfinalizer(lambda: instance2.stop())
    instance2.start()

    kafka_utils.wait_for_offset_catchup()


def test_when_batch_with_delay_blocks_next_batch(request, given_factory, given_kafka_up_to_date):
    log.info("Starting offset: " + str(kafka_utils.get_offsets()))
    log.info("Latest offsets: " + str(kafka_utils.get_latest_offsets()))

    for _ in range(5):
        kafka_utils.produce_element_with_delay(60000)

    instance1 = given_factory.generate_instance()
    instance1.start()
    kafka_utils.wait_until_consumer_group()
    sleep(5)

    kafka_utils.produce_element_with_delay(1000)

    kafka_utils.ensure_not_up_to_date_for_n_seconds(40)
