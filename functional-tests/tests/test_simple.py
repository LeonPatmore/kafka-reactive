import logging

import pytest

from cofiguration import kafka_utils, group_id
from kafka_processor_service import KafkaService
from service_starter import ServiceInstance


@pytest.fixture(scope="session", autouse=True)
def ensure_topic():
    kafka_utils.ensure_topic_created()


@pytest.fixture(params=[KafkaService()])
def given_service(request):
    return request.param


def test_simple(given_service):
    logging.info("Starting offset: " + str(kafka_utils.get_offsets(group_id)))

    kafka_utils.produce_element()

    instance = given_service.start()  # type: ServiceInstance

    # TODO: Check that the offset is fixed.

    instance.stop()
