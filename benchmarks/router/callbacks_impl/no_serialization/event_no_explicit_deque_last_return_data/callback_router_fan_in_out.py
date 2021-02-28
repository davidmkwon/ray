import ray
import asyncio
import time
from collections import deque, defaultdict
import uvloop
import click

# asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


@ray.remote
class NoopBackend:
    def __init__(self):
        pass

    def __call__(self, data):
        return data + 1


class Query:
    def __init__(self, inverted_pipeline, async_future=None):
        self.inverted_pipeline = inverted_pipeline
        self.current_state = defaultdict(list)
        for service in inverted_pipeline:
            self.clear(service)

        if async_future:
            self.async_future = async_future
        else:
            self.async_future = asyncio.get_event_loop().create_future()

    def addFullArg(self, service, args):
        self.current_state[service] = args

    def addArg(self, current_service, previous_service, arg):
        index = self.inverted_pipeline[current_service].index(previous_service)
        self.current_state[current_service][index] = arg

    def getArgs(self, service):
        assert all(self.current_state[service])
        return self.current_state[service]

    def isready(self, current_service):
        return all(self.current_state[current_service])

    def clear(self, current_service):
        if any(self.current_state[current_service]):
            self.current_state[current_service].clear()
        if self.inverted_pipeline[current_service]:
            for _ in range(len(self.inverted_pipeline[current_service])):
                self.current_state[current_service].append(None)


def register_callback(
    loop,
    objectref,
    router,
    dequeue_service,
    enqueue_service,
    first_async_future,
    is_last,
    query,
    replica_handle_id=None,
):
    def callback(completed_objectref):
        # print("ObjectRef complete")
        loop = first_async_future.get_loop()
        if replica_handle_id is not None:
            router.worker_queues[dequeue_service].append(replica_handle_id)
            loop.create_task(router.flush(dequeue_service))

        def callback_fn():
            if not is_last:
                query.addArg(
                    enqueue_service, dequeue_service, completed_objectref
                )
                if query.isready(enqueue_service):
                    router.service_queues[enqueue_service].append(query)
                    loop.create_task(router.flush(enqueue_service))
            else:
                first_async_future.set_result(completed_objectref)

        loop.call_soon_threadsafe(callback_fn)

    if is_last:
        objectref._on_completed(callback)
    else:
        objectref._on_completed(callback, deserialize=False)


# A dummy router for tackling
@ray.remote
class DequeRouter:
    def __init__(self, uvloop_flag=True):
        self.service_queues = defaultdict(deque)
        self.worker_queues = defaultdict(deque)
        self.worker_handles = defaultdict(list)

        self.max_batch_size_dict = dict()
        self._running = True
        if uvloop_flag:
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        # loop.create_task(

    async def set_pipeline(
        self, pipeline_dict, inverted_pipeline_dict, source, sink
    ):
        self.pipeline_dict = pipeline_dict
        self.inverted_pipeline_dict = inverted_pipeline_dict
        self.source = source
        self.sink = sink

    async def set_max_batch_size(self, service, max_batch_size):
        self.max_batch_size_dict[service] = max_batch_size

    async def dequeue_request(self, backend, replica_handle):
        replica_id = len(self.worker_handles[backend])
        self.worker_handles[backend].append(replica_handle)
        self.worker_queues[backend].append(replica_id)
        # asyncio.get_event_loop().create_task(self.flush(backend))
        # await self.flush(backend)

    async def enqueue_request(self, service, args, async_future=None):
        # print(f"Service Enqueue -> [{service}] ")
        is_first = service == self.source
        query = Query(self.inverted_pipeline_dict, async_future=async_future)
        if is_first:
            query.addFullArg(service, args)
        self.service_queues[service].append(query)
        # print("enqueue done")
        await self.flush(service)
        # asyncio.get_event_loop().create_task(self.flush(service))
        if is_first:
            # print("Waiting for result ......")
            result = await query.async_future
            # print("result returned")
            return result

    async def flush(self, service):
        buffer_queue = self.service_queues[service]
        worker_queue = self.worker_queues[service]
        # max_batch_size = self.max_batch_size_dict[service]
        max_batch_size = None

        while len(buffer_queue) and len(worker_queue):
            worker_id = worker_queue.popleft()
            worker = self.worker_handles[service][worker_id]
            if max_batch_size is None:  # No batching
                request = buffer_queue.popleft()
                ray_future = worker.__call__._remote(
                    args=request.getArgs(service), num_returns=1
                )
                for outgoing_service in self.pipeline_dict[service]:
                    register_callback(
                        loop=asyncio.get_event_loop(),
                        objectref=ray_future,
                        router=self,
                        dequeue_service=service,
                        enqueue_service=outgoing_service,
                        first_async_future=request.async_future,
                        is_last=False,
                        query=request,
                        replica_handle_id=worker_id,
                    )
                if service == self.sink:
                    register_callback(
                        loop=asyncio.get_event_loop(),
                        objectref=ray_future,
                        router=self,
                        dequeue_service=service,
                        enqueue_service=None,
                        first_async_future=request.async_future,
                        is_last=True,
                        query=request,
                        replica_handle_id=worker_id,
                    )


@click.command()
@click.option("--num-replicas", type=int, default=1)
@click.option("--uv", type=bool, default=True)
def driver(num_replicas, uv):
    print(f"[config] # Replicas: {num_replicas} uvloop: {uv}")
    if uv:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    for i in range(2):
        ray.init(
            _system_config={
                # "max_direct_call_object_size": 0,
                # "distributed_ref_counting_enabled": False,
                "record_ref_creation_sites": False,
            }
        )
        if i == 0:
            NUM_REPLICAS_A, NUM_REPLICAS_B = num_replicas, num_replicas

            router_handle = DequeRouter.remote(uv)
            source, sink = "A", "A"
            # pipeline = {"A": "B", "B": None}
            pipeline, inverted_pipeline = {"A": []}, {"A": []}
        elif i == 1:
            NUM_REPLICAS_A, NUM_REPLICAS_B = num_replicas, num_replicas
            # ray.init()
            router_handle = DequeRouter.remote(uv)
            source, sink = "A", "B"
            # pipeline = {"A": "B", "B": None}
            pipeline, inverted_pipeline = (
                {"A": ["B"], "B": []},
                {"A": [], "B": ["A"]},
            )

        # set the pipeline
        ray.get(
            router_handle.set_pipeline.remote(
                pipeline, inverted_pipeline, source, sink
            )
        )

        # create replicas for service A
        a_registered_replica_objectref = [
            router_handle.dequeue_request.remote("A", NoopBackend.remote())
            for _ in range(NUM_REPLICAS_A)
        ]
        ray.get(a_registered_replica_objectref)

        if i == 1:
            # create replicas for service B
            b_registered_replica_objectref = [
                router_handle.dequeue_request.remote("B", NoopBackend.remote())
                for _ in range(NUM_REPLICAS_B)
            ]
            ray.get(b_registered_replica_objectref)

        mean_qps = 0.0
        AVG_CALC = 7
        mean_closed_loop = 0.0
        CLOSED_LOOP_LATENCY_ITER = 500
        for _ in range(AVG_CALC):
            # throughput measurement
            WARMUP, NUM_REQUESTS = 200, 1000
            future = [
                router_handle.enqueue_request.remote("A", [1])
                for _ in range(WARMUP)
            ]
            ray.wait(future, num_returns=WARMUP)
            del future

            futures = [
                router_handle.enqueue_request.remote("A", [1])
                for _ in range(NUM_REQUESTS)
            ]

            start_time = time.perf_counter()
            ray.wait(futures, num_returns=NUM_REQUESTS)
            # get_data(futures)
            end_time = time.perf_counter()
            duration = end_time - start_time
            qps = NUM_REQUESTS / duration
            mean_qps += qps
            sum_closed_loop = 0.0
            for _ in range(CLOSED_LOOP_LATENCY_ITER):
                start = time.perf_counter()
                ray.get(router_handle.enqueue_request.remote("A", [1]))
                end = time.perf_counter()
                sum_closed_loop += end - start
            mean_closed_loop += sum_closed_loop / CLOSED_LOOP_LATENCY_ITER
            del futures

        final_qps = mean_qps / AVG_CALC
        mean_closed_loop = mean_closed_loop / AVG_CALC
        print(f"Pipeline Length: {i+1}")
        print(
            f"Throughput QPS: {final_qps} A Replicas: {NUM_REPLICAS_A} "
            f"Mean Closed Loop Latency: {mean_closed_loop} "
            f"B Replicas: {NUM_REPLICAS_B}"
        )
        print(ray.get(router_handle.enqueue_request.remote("A", [100])))
        ray.shutdown()
    # return qps


if __name__ == "__main__":
    driver()
