import logging
import os
import signal
import threading
from commands import run
from service_starter import ServiceInstance, ServiceFactory

log = logging.getLogger(__name__)


class ProcessInstance(ServiceInstance):

    class _Thread(threading.Thread):

        def __init__(self, cmd: str):
            super().__init__()
            self.process = None
            self.cmd = cmd

        def run(self):
            self.process = run(self.cmd)

        def kill(self):
            # os.kill(self.pid, signal.CTRL_C_EVENT)
            pass

        def stop(self):
            os.system(f"taskkill /pid {self.process.pid} /f /t")
            # threading.Thread(target=lambda: self.kill()).start()
            pass

    def __init__(self, cmd):
        self.thread = None
        self.cmd = cmd

    def start(self):
        self.thread = self._Thread(self.cmd)
        self.thread.start()

    def stop(self):
        log.info("Killing service!")
        self.thread.stop()


class BatchConsumerFactory(ServiceFactory):

    def generate_instance(self) -> ServiceInstance:
        return ProcessInstance("..\\..\\kafka-batch-consumer\\gradlew -b "
                               "..\\..\\kafka-batch-consumer\\build.gradle.kts "
                               "test --tests *TestConsumer*")
