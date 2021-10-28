
class ServiceInstance(object):

    def start(self):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()


class ServiceFactory(object):

    def generate_instance(self) -> ServiceInstance:
        raise NotImplementedError()
