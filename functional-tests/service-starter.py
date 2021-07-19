
class ServiceInstance(object):

    def stop(self):
        raise NotImplementedError()


class Service(object):

    def start(self) -> ServiceInstance:
        raise NotImplementedError()
