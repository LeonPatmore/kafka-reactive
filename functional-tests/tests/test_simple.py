import pytest

from cofiguration import kafka_utils
from kafka_processor_service import KafkaService


@pytest.fixture(params=[KafkaService()])
def given_service(request):
    return request.param


def test_simple(given_service):
    kafka_utils.produce_element()
