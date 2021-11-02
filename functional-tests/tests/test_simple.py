import logging
from time import sleep

import pytest

from cofiguration import kafka_utils
from kafka_processor_service import BatchConsumerFactory
from service_starter import ServiceInstance

logging.getLogger("kafka.conn").setLevel(logging.DEBUG)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


@pytest.fixture(scope="session", autouse=True)
def ensure_topic():
    kafka_utils.ensure_topic_created()


@pytest.fixture(params=[BatchConsumerFactory()])
def given_service(request) -> ServiceInstance:
    instance = request.param.generate_instance()
    request.addfinalizer(lambda: instance.stop())
    return instance


@pytest.fixture
def given_kafka_up_to_date():
    kafka_utils.consume_messages_and_close()


def test_simple(given_service, given_kafka_up_to_date):
    log.info("Starting offset: " + str(kafka_utils.get_offsets()))
    log.info("Latest offsets: " + str(kafka_utils.get_latest_offsets()))
    kafka_utils.ensure_group_up_to_date()

    kafka_utils.produce_element_with_delay(1000)

    log.info("New offset: " + str(kafka_utils.get_offsets()))
    log.info("New latest offsets: " + str(kafka_utils.get_latest_offsets()))

    given_service.start()

    kafka_utils.wait_for_offset_catchup()

    given_service.stop()


def test_when_instance_dies(given_service, given_kafka_up_to_date):
    log.info("Starting offset: " + str(kafka_utils.get_offsets()))
    log.info("Latest offsets: " + str(kafka_utils.get_latest_offsets()))
    kafka_utils.ensure_group_up_to_date()

    kafka_utils.produce_element_with_delay(15000)

    log.info("New offset: " + str(kafka_utils.get_offsets()))
    log.info("New latest offsets: " + str(kafka_utils.get_latest_offsets()))

    given_service.start()

    sleep(10)

    given_service.stop()

    sleep(1)

    kafka_utils.wait_for_offset_catchup()


def test_():
    BatchConsumerFactory().generate_instance().start()
