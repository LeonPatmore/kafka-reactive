import os
import signal
import threading
from commands import run
from service_starter import ServiceInstance, ServiceFactory


class ProcessInstance(ServiceInstance):

    class _Thread(threading.Thread):

        def __init__(self, cmd: str):
            super().__init__()
            self.pid = None
            self.cmd = cmd

        def run(self):
            self.pid = run(self.cmd)

        def stop(self):
            os.kill(self.pid, signal.CTRL_C_EVENT)

    def __init__(self, cmd):
        self.thread = None
        self.cmd = cmd

    def start(self):
        self.thread = self._Thread(self.cmd)
        self.thread.start()

    def stop(self):
        self.thread.stop()
        self.thread.join()


class BatchConsumerFactory(ServiceFactory):

    def generate_instance(self) -> ServiceInstance:
        return ProcessInstance("..\\..\\kafka-batch-consumer\\gradlew -b "
                               "..\\..\\kafka-batch-consumer\\build.gradle.kts "
                               "test --tests *TestConsumer*")
