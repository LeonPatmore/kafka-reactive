import logging
import os
import threading

from commands import run
from service_starter import ServiceInstance, ServiceFactory

log = logging.getLogger(__name__)


class LocalProcessInstance(ServiceInstance):

    class _Thread(threading.Thread):

        def __init__(self, cmd: str):
            super().__init__()
            self.process = None
            self.cmd = cmd

        def run(self):
            self.process = run(self.cmd)

        def stop(self):
            os.system(f"taskkill /pid {self.process.pid} /f /t")

    def __init__(self, folder: str):
        self.thread = None
        self.cmd = self._get_cmd(folder)

    @staticmethod
    def _get_cmd(folder: str):
        # TODO: Max dynamic.
        os = "mac"
        if os is "mac":
            return f"../../{folder}/gradlew -b ../../{folder}/build.gradle.kts test --tests *TestConsumer*"
        else:
            return f"..\\..\\{folder}\\gradlew -b ..\\..\\{folder}\\build.gradle.kts test --tests *TestConsumer*"

    def start(self):
        log.info("Starting service!")
        self.thread = self._Thread(self.cmd)
        self.thread.start()

    def stop(self):
        log.info("Killing service!")
        self.thread.stop()
        self.thread.join()


class KafkaParallelConsumerFactory(ServiceFactory):

    def generate_instance(self) -> ServiceInstance:
        return LocalProcessInstance("kafka-lib")


class BatchConsumerFactory(ServiceFactory):

    def generate_instance(self) -> ServiceInstance:
        return LocalProcessInstance("kafka-batch-consumer")


class SpringReactorFactory(ServiceFactory):

    def generate_instance(self) -> ServiceInstance:
        return LocalProcessInstance("kafka-reactive-spring")
