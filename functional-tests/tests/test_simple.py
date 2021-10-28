import logging

import pytest

from cofiguration import kafka_utils, group_id
from kafka_processor_service import BatchConsumerFactory
from service_starter import ServiceInstance

logging.getLogger("kafka.conn").setLevel(logging.WARN)
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


def test_simple(given_service):
    log.info("Starting offset: " + str(kafka_utils.get_offsets(group_id)))
    log.info("Latest offsets: " + str(kafka_utils.get_latest_offsets()))
    kafka_utils.ensure_group_up_to_date(group_id)

    kafka_utils.produce_random_element()

    log.info("New offset: " + str(kafka_utils.get_offsets(group_id)))
    log.info("New latest offsets: " + str(kafka_utils.get_latest_offsets()))

    given_service.start()

    kafka_utils.wait_for_offset_catchup(group_id)

    given_service.stop()
