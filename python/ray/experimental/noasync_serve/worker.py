import ray

@ray.remote
class Worker:
    """
    Very basic draft of worker replica in router
    """
    def __init__(self, name, func, router_handle):
        # TODO: add more fields
        self.name = name
        self.func = func
        self._router_handle = router_handle
        self._my_handle = None

    def poll_router(self, my_handle):
        self._my_handle = my_handle

        # get an empty work intent from router
        work_intent = ray.get(
            self._router_handle.dequeue_request.remote(
                self.name
            )
        )

        # wait for router to put request into work intent
        #request = ray.get(ray.ObjectID(work_intent))
        request = ray.get(work_intent)
        request_result_object_id = request.result_object_id
        request_body = request.request_body
        print("received {}".format(request_body))

        # execute request
        output = self.func(request_body)

        # put output into result_id
        #ray.worker.global_worker.put_object(output, request_result_object_id)
        ray.put(output, request_result_object_id)

        # recursively poll router again
        self._my_handle.poll_router.remote(my_handle)
