import psutil

from commands import run
from service_starter import Service, ServiceInstance


class ProcessInstance(ServiceInstance):

    def __init__(self, pid: int):
        self.pid = pid

    def stop(self):
        proc = psutil.Process(self.pid)
        proc.terminate()


class KafkaService(Service):

    def start(self) -> ServiceInstance:
        pid = run("./gradlew test --tests *TestConsumer* --stacktrace")
        return ProcessInstance(pid)
