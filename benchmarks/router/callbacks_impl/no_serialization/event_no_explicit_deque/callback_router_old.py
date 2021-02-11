"""
Design 1:
    - Pipeline Orchestration with no batching
    - Uses callbacks attached to ObjectRefs
        - Implemented a mechanism to disable serialization from callbacks
    # RPCs -> driver -> router -> replica (critical path)
    #         replica -> router 
             
"""
import ray
import asyncio
import time
from collections import deque, defaultdict
import uvloop
import click
import random

# asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


@ray.remote
class NoopBackend:
    def __init__(self):
        pass

    def __call__(self, data):
        return data + 1


class Query:
    def __init__(self, args, async_future=None):
        self.args = args
        if async_future:
            self.async_future = async_future
        else:
            self.async_future = asyncio.get_event_loop().create_future()


def register_callback(
    loop,
    objectref,
    router,
    dequeue_service,
    enqueue_service,
    first_async_future,
    event,
    replica_handle=None,
):
    def callback(completed_objectref):
        # print("ObjectRef complete")
        loop = first_async_future.get_loop()
        if replica_handle is not None:
            # print("Calling Flush")
            router.worker_available[dequeue_service][replica_handle] = True
            loop.create_task(
                router.flush(dequeue_service)
            )  # , replica_handle))
        if enqueue_service is not None:
            loop.create_task(
                router.enqueue_request(
                    enqueue_service,
                    [completed_objectref],
                    first_async_future,
                    event,
                )
            )
        else:

            def set_future():
                # print("Result set")
                first_async_future.set_result(completed_objectref)

            loop.call_soon_threadsafe(set_future)

        def set_event():
            # print("event set")
            event.set()

        loop.call_soon_threadsafe(set_event)

    objectref._on_completed(callback, deserialize=False)


# A dummy router for tackling
@ray.remote
class DequeRouter:
    def __init__(self, uvloop_flag=True):
        self.service_queues = defaultdict(deque)
        self.worker_queues = defaultdict(list)
        self.worker_available = defaultdict(list)
        self.max_batch_size_dict = dict()
        self._running = True
        self._flush_calls = 0
        if uvloop_flag:
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        # loop.create_task(

    async def set_pipeline(self, pipeline_dict, source):
        self.pipeline_dict = pipeline_dict
        self.source = source

    async def set_max_batch_size(self, service, max_batch_size):
        self.max_batch_size_dict[service] = max_batch_size

    async def register_replica(self, backend, replica_handle):
        rid = len(self.worker_available[backend])
        self.worker_queues[backend].append((rid, replica_handle))
        self.worker_available[backend].append(True)
        # asyncio.get_event_loop().create_task(self.flush(backend))
        # await self.flush(backend)

    async def enqueue_request(
        self, service, args, async_future=None, event=None
    ):
        if event is not None:
            await event.wait()
        # print(f"Service Enqueue -> [{service}] ")
        is_first = service == self.source
        query = Query(args, async_future=async_future)
        self.service_queues[service].append(query)
        # print("enqueue done")
        await self.flush(service)
        # asyncio.get_event_loop().create_task(self.flush(service))
        if is_first:
            # print("Waiting for result ......")
            result = await query.async_future
            # print("result returned")
            return result

    async def flush(self, service, rid=None):
        if rid is not None:
            # print(f"Done: {rid}")
            self.worker_available[service][rid] = True
        # self._flush_calls += 1
        buffer_queue = self.service_queues[service]
        worker_queue = self.worker_queues[service]
        # max_batch_size = self.max_batch_size_dict[service]
        max_batch_size = None
        # successful_flush_event_signal = []
        # print(
        #     f"[Flush call: {self._flush_calls}] {service} "
        #     f"Worker Available: {self.worker_available[service]}"
        # )

        while len(buffer_queue) and any(self.worker_available[service]):
            for rid, worker in random.sample(worker_queue, len(worker_queue)):
                if not self.worker_available[service][rid]:
                    continue
                # worker = worker_queue.popleft()
                if max_batch_size is None:  # No batching
                    request = buffer_queue.popleft()
                    ray_future = worker.__call__._remote(
                        args=request.args, num_returns=1
                    )
                    event = asyncio.Event()

                    register_callback(
                        loop=asyncio.get_event_loop(),
                        objectref=ray_future,
                        router=self,
                        dequeue_service=service,
                        enqueue_service=self.pipeline_dict[service],
                        first_async_future=request.async_future,
                        event=event,
                        replica_handle=rid,
                    )
                    # successful_flush_event_signal.append(event)
                    self.worker_available[service][rid] = False
                    break

        # await asyncio.gather(
        #     *[e.wait() for e in successful_flush_event_signal]
        # )
        # await asyncio.sleep(0.01)

        # else:
        #     real_batch_size = min(len(buffer_queue), max_batch_size)
        #     requests = [buffer_queue.pop(0) for _ in range(real_batch_size)]
        #     unwrapped_kwargs = to_batchable_kwargs(requests)

        #     ray_futures = worker._ray_serve_call_ref._remote(
        #         args=[],
        #         kwargs={
        #             "metadata": {
        #                 "kwarg_keys": list(
        #                     requests[0].request_kwargs.keys()
        #                 ),
        #                 "batch_size": real_batch_size,
        #                 "call_method": "__call__",
        #             },
        #             **unwrapped_kwargs,
        #         },
        #         num_return_vals=real_batch_size,
        #     )

        #     if real_batch_size == 1:
        #         ray_futures = [ray_futures]

        #     for batch_idx in range(real_batch_size):
        #         requests[batch_idx].async_future.set_result(
        #             ray_futures[batch_idx]
        #         )


def get_data(futures):
    # start_time = time.perf_counter()
    all_ready = False
    num_requests = len(futures)
    current_router = futures
    current_result = list()
    all_ready = False
    cnt = 0
    cnt_all_ready = 0
    while True:
        if not all_ready:
            ready, unready = ray.wait(
                current_router, num_returns=len(current_router), timeout=0
            )
            cnt_all_ready += len(ready)
        else:
            ready, unready = [], []

        if all_ready or len(ready) > 0:
            result_wait = ray.get(ready) + current_result
            s_ready, s_unready = ray.wait(
                result_wait, num_returns=len(result_wait), timeout=0
            )
            cnt += len(s_ready)
            if cnt == num_requests:
                assert len(s_unready) == 0, "Wrong throughput calculation"
                break
            else:
                current_result = s_unready
        if len(unready) > 0:
            current_router = unready
        else:
            all_ready = True
            assert (
                cnt_all_ready == num_requests
            ), "Wrong throughput calculation"
            # print(f"All fired queries ready: {cnt_all_ready}")
            # current_router = s_unready

    # end_time = time.perf_counter()
    # duration = end_time - start_time
    # qps = num_requests / duration
    # return qps


@click.command()
@click.option("--num-replicas", type=int, default=1)
@click.option("--uv", type=bool, default=True)
def driver(num_replicas, uv):
    print(f"[config] # Replicas: {num_replicas} uvloop: {uv}")
    if uv:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    for i in range(2):
        ray.init()
        if i == 0:
            NUM_REPLICAS_A, NUM_REPLICAS_B = num_replicas, num_replicas

            router_handle = DequeRouter.remote(uv)
            source = "A"
            # pipeline = {"A": "B", "B": None}
            pipeline_1 = {"A": None}
        elif i == 1:
            NUM_REPLICAS_A, NUM_REPLICAS_B = num_replicas, num_replicas
            # ray.init()
            router_handle = DequeRouter.remote(uv)
            source = "A"
            # pipeline = {"A": "B", "B": None}
            pipeline_1 = {"A": "B", "B": None}

        # set the pipeline
        ray.get(router_handle.set_pipeline.remote(pipeline_1, source))

        # create replicas for service A
        a_registered_replica_objectref = [
            router_handle.register_replica.remote("A", NoopBackend.remote())
            for _ in range(NUM_REPLICAS_A)
        ]
        ray.get(a_registered_replica_objectref)

        if i == 1:
            # create replicas for service B
            b_registered_replica_objectref = [
                router_handle.register_replica.remote(
                    "B", NoopBackend.remote()
                )
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
            ray.wait(ray.get(future), num_returns=WARMUP)
            del future

            futures = [
                router_handle.enqueue_request.remote("A", [1])
                for _ in range(NUM_REQUESTS)
            ]

            start_time = time.perf_counter()
            get_data(futures)
            end_time = time.perf_counter()
            duration = end_time - start_time
            qps = NUM_REQUESTS / duration
            mean_qps += qps
            sum_closed_loop = 0.0
            for _ in range(CLOSED_LOOP_LATENCY_ITER):
                start = time.perf_counter()
                ray.get(
                    ray.get(router_handle.enqueue_request.remote("A", [1]))
                )
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
        print(
            ray.get(ray.get(router_handle.enqueue_request.remote("A", [100])))
        )

        ray.shutdown()
    # return qps


if __name__ == "__main__":
    driver()
