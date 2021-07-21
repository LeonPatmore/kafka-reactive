from commands import run
from service_starter import Service, ServiceInstance


class KafkaService(Service):

    def start(self) -> ServiceInstance:
        run("./gradlew ")
