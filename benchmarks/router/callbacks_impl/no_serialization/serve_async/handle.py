class RayServeHandle:
    def __init__(
        self,
        router_handle,
        source,
    ):
        self.router_handle = router_handle
        self.pipeline_source = source

    def remote(self, args):
        return self.router_handle.enqueue_request.remote(self.pipeline_source, [args])

    def enqueue_batch(self, **kwargs):
        for k, v in kwargs.items():
            assert isinstance(v, list), f"Input to argument {k} is not a list"

        return self.router_handle.enqueue_batch.remote()
