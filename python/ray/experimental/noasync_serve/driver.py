import click

import ray
from queues import CentralizedQueuesActor
from worker import Worker

@click.command()
@click.option("--num-replicas", type=int, default=1)
#@click.option("--open-mean-qps", type=int, default=100)
def driver(num_replicas):
    print(f"[config] # Replicas: {num_replicas}")
    ray.init(
        _system_config={
             "enable_timeline": False,
            "record_ref_creation_sites": False,
        }
    )

    #NUM_REPLICAS_A, NUM_REPLICAS_B = num_replicas, num_replicas
    NUM_REPLICAS = num_replicas
    NUM_REQUESTS = 1
    worker_name = "Hello World"
    router_handle = CentralizedQueuesActor.remote()
    ray.get(router_handle.link.remote(worker_name, worker_name))

    # create replicas for HelloWorld service and "start" them
    def worker_func(data):
        return "Hello, World: " + data

    for _ in range(NUM_REPLICAS):
        worker_handle = Worker.remote(worker_name, worker_func, router_handle)
        worker_handle.poll_router.remote(worker_handle)

    # enqueue NUM_REQUEST queries and retrieve their outputs
    output_futures = [
        router_handle.enqueue_request.remote(worker_name, "David")
        for _ in range(NUM_REQUESTS)
    ]
    ray.wait(output_futures, num_returns=NUM_REQUESTS)
    for i in range(NUM_REQUESTS):
        output = ray.get(output_futures[i])
        output2 = ray.get(output)
        print(output2)

    '''mean_qps = 0.0
    AVG_CALC = 1
    mean_closed_loop = 0.0
    CLOSED_LOOP_LATENCY_ITER = 500
    for _ in range(AVG_CALC):
        # throughput measurement
        WARMUP, NUM_REQUESTS = 200, 2000
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

    # open loop benchmarking
    import subprocess
    from server import HTTPProxyActor
    import requests
    from utils import get_latency_stats, generate_fixed_arrival_process

    # Open Loop
    cv = 4
    arrival_curve = generate_fixed_arrival_process(
        mean_qps=open_mean_qps, cv=cv, num_requests=2000,
    ).tolist()

    http_actor = HTTPProxyActor.remote(host="127.0.0.1", port=8000)
    ray.get(
        http_actor.register_route.remote("/resnet50", router_handle, source)
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

    with open('./latency/ol-latency-{}-2-{}-{}'.format(open_mean_qps, num_replicas, cv), 'w') as f:
        for item in latency_ms:
            f.write("{}\n".format(str(item)))'''

    ray.shutdown()

if __name__ == "__main__":
    driver()
