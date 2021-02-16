from typing import Any
from torch.autograd import Variable
from PIL import Image
import base64
import torch
import torchvision.transforms as transforms
from torchvision import models
import io
import os
import ray
from pprint import pprint
import warnings

import pandas as pd
import click
import json

import asyncio
import time
from collections import deque, defaultdict
import uvloop
import click
import numpy as np


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
        # print("ObjectRef complete")
        loop = first_async_future.get_loop()
        if replica_handle_id is not None:
            router.worker_queues[dequeue_service].append(replica_handle_id)
            loop.create_task(router.flush(dequeue_service))
        if event is None:
            #     loop.create_task(
            #         router.enqueue_request(
            #             enqueue_service,
            #             [completed_objectref],
            #             first_async_future,
            #             event,
            #         )
            #     )
            # else:

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


class Query:
    def __init__(self, args, async_future=None):
        self.args = args
        if async_future:
            self.async_future = async_future
        else:
            self.async_future = asyncio.get_event_loop().create_future()


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

    async def set_pipeline(self, pipeline_dict, source):
        self.pipeline_dict = pipeline_dict
        self.source = source

    async def set_max_batch_size(self, service, max_batch_size):
        self.max_batch_size_dict[service] = max_batch_size

    async def dequeue_request(self, backend, replica_handle):
        replica_id = len(self.worker_handles[backend])
        self.worker_handles[backend].append(replica_handle)
        self.worker_queues[backend].append(replica_id)
        # asyncio.get_event_loop().create_task(self.flush(backend))
        # await self.flush(backend)

    async def enqueue_request(
        self, service, *args, async_future=None, event=None
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
                            ray_future,
                            async_future=request.async_future,
                            event=event,
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


"""
Ideally the above code will be part of library.
The below code will be written by SRTML user.
"""


@ray.remote
class Transform:
    """
    Standard pytorch pre-processing functionality
    - gets a raw image
    - converts it to tensor
    """

    def __init__(self, transform: Any) -> None:

        self.transform = transform

    def __call__(self, data: list) -> list:
        # data_list = list()
        # for img in data:
        data = Image.open(io.BytesIO(data))
        if data.mode != "RGB":
            data = data.convert("RGB")
        data = self.transform(data)
        return data.numpy()
        #     data_list.append(data)
        # return data_list


@ray.remote
class PredictModelPytorch:
    """
    Standard pytorch prediction functionality
    - gets a preprocessed tensor
    - predicts it's class
    """

    def __init__(self, model_name: str, is_cuda: bool = False) -> None:
        self.model = models.__dict__[model_name](pretrained=True)
        self.is_cuda = is_cuda
        warnings.filterwarnings("ignore")
        if is_cuda:
            self.model = self.model.cuda()

    def __call__(self, data: list) -> list:
        data = torch.stack([torch.from_numpy(data)])
        data = Variable(data)
        if self.is_cuda:
            data = data.cuda()
        outputs = self.model(data)
        _, predicted = outputs.max(1)
        return predicted.cpu().numpy().tolist()[0]


@click.command()
@click.option("--num-replicas", type=int, default=1)
@click.option("--uv", type=bool, default=True)
def driver(num_replicas, uv):
    print(f"[config] # Replicas: {num_replicas} uvloop: {uv}")
    if uv:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    ray.init(
        _system_config={
           # "max_direct_call_object_size": 112*1024,
             "enable_timeline": False,
            # "debug_dump_period_milliseconds": -1,
            # "distributed_ref_counting_enabled": False,
            "record_ref_creation_sites": False,
        }
    )

    NUM_REPLICAS_A, NUM_REPLICAS_B = num_replicas, num_replicas
    # ray.init()
    router_handle = DequeRouter.remote(uv)
    source = "preprocess"
    # pipeline = {"A": "B", "B": None}
    pipeline_1 = {"preprocess": "classification", "classification": None}

    # set the pipeline
    ray.get(router_handle.set_pipeline.remote(pipeline_1, source))

    min_img_size = 224
    transform = transforms.Compose(
        [
            transforms.Resize(min_img_size),
            transforms.ToTensor(),
            transforms.Normalize(
                mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225],
            ),
        ]
    )
    # create replicas for service A
    a_registered_replica_objectref = [
        router_handle.dequeue_request.remote(
            source, Transform.remote(transform)
        )
        for _ in range(NUM_REPLICAS_A)
    ]
    ray.get(a_registered_replica_objectref)

    # create replicas for service B
    b_registered_replica_objectref = [
        router_handle.dequeue_request.remote(
            "classification",
            PredictModelPytorch.options(num_gpus=1).remote("resnet50", True),
        )
        for _ in range(NUM_REPLICAS_B)
    ]
    ray.get(b_registered_replica_objectref)

    img = open("elephant.jpg", "rb").read()

    mean_qps = 0.0
    AVG_CALC = 1
    mean_closed_loop = 0.0
    CLOSED_LOOP_LATENCY_ITER = 500
    for _ in range(AVG_CALC):
        # throughput measurement
        WARMUP, NUM_REQUESTS = 200, 1000
        future = [
            router_handle.enqueue_request.remote(source, img)
            for _ in range(WARMUP)
        ]
        ray.wait(future, num_returns=WARMUP)
        del future

        futures = [
            router_handle.enqueue_request.remote(source, img)
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
            ray.get(router_handle.enqueue_request.remote(source, img))
            end = time.perf_counter()
            sum_closed_loop += end - start
        mean_closed_loop += sum_closed_loop / CLOSED_LOOP_LATENCY_ITER
        del futures

    final_qps = mean_qps / AVG_CALC
    mean_closed_loop = mean_closed_loop / AVG_CALC
    print(f"Image Preprocessing Pipeline")
    print(
        f"Throughput QPS: {final_qps} Prepoc Replicas: {NUM_REPLICAS_A} "
        f"Mean Closed Loop Latency: {mean_closed_loop} "
        f"Classifier Replicas: {NUM_REPLICAS_B}"
    )
    print(ray.get(router_handle.enqueue_request.remote(source, img)))
    ray.shutdown()
    # return qps


if __name__ == "__main__":
    driver()
