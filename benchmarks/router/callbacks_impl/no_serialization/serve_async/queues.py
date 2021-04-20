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
import uvloop
import click
from collections import deque, defaultdict


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
    first_async_future,
    event=None,
    replica_handle_id=None,
):
    def callback(completed_objectref):
        loop = first_async_future.get_loop()
        if replica_handle_id is not None:
            router.worker_queues[dequeue_service].append(replica_handle_id)
            loop.create_task(router.flush(dequeue_service))
        if event is None:
            def set_future():
                # print("Result set", flush=True)
                first_async_future.set_result(completed_objectref)
            loop.call_soon_threadsafe(set_future)
        else:
            def set_event():
                event.set()
            loop.call_soon_threadsafe(set_event)

    if event is None:
        objectref._on_completed(callback)
    else:
        objectref._on_completed(callback, deserialize=False)


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

    def set_pipeline(self, pipeline_dict):
        self.pipeline_dict = pipeline_dict

    def set_source(self, source):
        self.source = source

    def get_pipeline(self):
        return self.pipeline_dict

    def get_source(self):
        return self.source

    async def set_max_batch_size(self, service, max_batch_size):
        self.max_batch_size_dict[service] = max_batch_size

    async def dequeue_request(self, backend, replica_handle):
        replica_id = len(self.worker_handles[backend])
        self.worker_handles[backend].append(replica_handle)
        self.worker_queues[backend].append(replica_id)

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
                    args=request.args, num_returns=1
                )
                event = None
                if self.pipeline_dict[service] is not None:
                    event = asyncio.Event()
                    asyncio.get_event_loop().create_task(
                        self.enqueue_request(
                            self.pipeline_dict[service],
                            [ray_future],
                            request.async_future,
                            event,
                        )
                    )

                register_callback(
                    loop=asyncio.get_event_loop(),
                    objectref=ray_future,
                    router=self,
                    dequeue_service=service,
                    first_async_future=request.async_future,
                    event=event,
                    replica_handle_id=worker_id,
                )

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
