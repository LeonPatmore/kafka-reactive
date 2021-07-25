import pytest

from cofiguration import kafka_utils
from kafka_processor_service import KafkaService
from service_starter import ServiceInstance


@pytest.fixture(params=[KafkaService()])
def given_service(request):
    return request.param


def test_simple(given_service):
    kafka_utils.produce_element()

    instance = given_service.start()  # type: ServiceInstance

    # TODO: Check that the offset is fixed.

    instance.stop()
