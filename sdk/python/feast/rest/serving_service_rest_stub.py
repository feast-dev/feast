class ServingServiceRESTStub(object):
    def __init__(self, serving_url) -> None:
        super().__init__()
        self.url = serving_url
        self.service_name = "feast.serving.ServingService"