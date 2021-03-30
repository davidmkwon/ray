import click
import time
import torchvision.transforms as transforms

import ray
from utils import get_data
from queues import CentralizedQueuesActor
from worker import ImagePreprocWorker

@click.command()
@click.option("--num-replicas", type=int, default=1)
@click.option("--open-mean-qps", type=int, default=100)
def driver(num_replicas, open_mean_qps):
    print(f"[config] # Replicas: {num_replicas}")
    ray.init(
        _system_config={
             "enable_timeline": False,
            "record_ref_creation_sites": False,
        }
    )

    NUM_REPLICAS = num_replicas
    NUM_REQUESTS = 100
    worker_name = "ImagePreproc"
    router_handle = CentralizedQueuesActor.remote()
    ray.get(router_handle.link.remote(worker_name, worker_name))
    #ray.get(router_handle.register_self_handle.remote(router_handle))

    # start workers with given model and transform
    min_img_size = 224
    transform = transforms.Compose(
        [
            transforms.Resize(min_img_size),
            transforms.ToTensor(),
            transforms.Normalize(
                mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.224],
            ),
        ]
    )
    model_name = "resnet50"
    is_cuda = True

    for _ in range(NUM_REPLICAS):
        worker_handle = ImagePreprocWorker.options(num_gpus=1).remote(
            worker_name, router_handle, transform, model_name, is_cuda
        )
        ray.get(worker_handle.set_my_handle.remote(worker_handle))
        worker_handle.start.remote()

    # image to pass through pipeline
    img = open("elephant.jpg", "rb").read()

    mean_qps = 0.0
    AVG_CALC = 1
    mean_closed_loop = 0.0
    CLOSED_LOOP_LATENCY_ITER = 500
    lstart = time.perf_counter()
    for _ in range(AVG_CALC):
        # throughput measurement
        WARMUP, NUM_REQUESTS = 200, 2000
        future = [
            router_handle.enqueue_request.remote(worker_name, img)
            for _ in range(WARMUP)
        ]
        ray.wait(future, num_returns=WARMUP)
        ray.wait([ray.get(oid) for oid in future], num_returns=WARMUP)
        del future

        futures = [
            router_handle.enqueue_request.remote(worker_name, img)
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
            ray.get(ray.get(router_handle.enqueue_request.remote(worker_name, img)))
            end = time.perf_counter()
            sum_closed_loop += end - start
        mean_closed_loop += sum_closed_loop / CLOSED_LOOP_LATENCY_ITER
        del futures

    lend = time.perf_counter()
    final_qps = mean_qps / AVG_CALC
    mean_closed_loop = mean_closed_loop / AVG_CALC
    print(f"Image Preprocessing Pipeline")
    print(
        f"Throughput QPS: {final_qps} ImageClassification Replicas: {NUM_REPLICAS} "
        f"Mean Closed Loop Latency: {mean_closed_loop} "
    )
    print(ray.get(ray.get(router_handle.enqueue_request.remote(worker_name, img))))
    print("took {}s".format(lend - lstart))

    '''# open loop benchmarking
    import subprocess
    from server import HTTPProxyActor
    import requests
    from utils import get_latency_stats, generate_fixed_arrival_process

    # Open Loop
    cv = 0
    arrival_curve = generate_fixed_arrival_process(
        mean_qps=open_mean_qps, cv=cv, num_requests=2000,
    ).tolist()

    http_actor = HTTPProxyActor.remote(host="127.0.0.1", port=8000)
    ray.get(
        http_actor.register_route.remote("/resnet50", router_handle, worker_name)
    )
    ray.get(http_actor.init_latency.remote())
    ls_output = subprocess.Popen(
        [
            "go",
            "run",
            "client.go",
            "60.0",
            "elephant.jpg",
            *[str(val) for val in arrival_curve],
        ]
    )
    ls_output.communicate()
    latency_list = ray.get(http_actor.get_latency.remote())
    ingest_mu, latency_ms, p95_ms, p99_ms = get_latency_stats(
        collected_latency=latency_list
    )
    print(f"p95(ms): {p95_ms} p99(ms): {p99_ms}")

    with open('./latency/ol-latency-{}-1-{}-{}'.format(open_mean_qps, num_replicas, cv), 'w') as f:
        for item in latency_ms:
            f.write("{}\n".format(str(item)))'''

    ray.shutdown()

if __name__ == "__main__":
    driver()
