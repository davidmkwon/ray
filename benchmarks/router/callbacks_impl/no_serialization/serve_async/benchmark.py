import click
import time
import ray

import api as serve

@click.command()
@click.option("--num-warmups", type=int, default=200)
@click.option("--num-queries", type=int, default=5000)
@click.option("--num-replicas", type=int, default=1)
@click.option("--uv", type=bool, default=True)
def driver(num_warmups, num_queries, num_replicas, uv):
    serve.init(
        {
            "_system_config":{
                "record_ref_creation_sites": False,
            }
        }
    )

    def noop(args):
        return args + 1

    NUM_REPLICAS_A, NUM_REPLICAS_B = num_replicas, num_replicas
    config_A = serve.BackendConfig(num_replicas=NUM_REPLICAS_A)
    config_B = serve.BackendConfig(num_replicas=NUM_REPLICAS_B)
    pipeline_dict = {"A":"B", "B":None}
    source = "A"

    with serve.using_router("noop"):
        serve.create_endpoint(noop, "A", config_A)
        serve.create_endpoint(noop, "B", config_B)
        serve.set_pipeline(pipeline_dict, source)
        serve_handle = serve.get_handle()

    print("done!")
    mean_qps = 0.0
    AVG_CALC = 7
    mean_closed_loop = 0.0
    CLOSED_LOOP_LATENCY_ITER = 500
    for _ in range(AVG_CALC):
        # do warmup queries
        futures = [
            serve_handle.remote(args=1)
            for _ in range(num_warmups)
        ]
        ray.wait(futures, num_returns=num_warmups)
        del futures

        futures = [
            serve_handle.remote(args=1)
            for _ in range(num_queries)
        ]
        start_time = time.perf_counter()
        ray.wait(futures, num_returns=num_queries)
        end_time = time.perf_counter()
        duration = end_time - start_time
        qps = num_queries / duration
        mean_qps += qps
        del futures

        sum_closed_loop = 0.0
        for _ in range(CLOSED_LOOP_LATENCY_ITER):
            start = time.perf_counter()
            ray.get(serve_handle.remote(args=1))
            end = time.perf_counter()
            sum_closed_loop += end - start
        mean_closed_loop += sum_closed_loop / CLOSED_LOOP_LATENCY_ITER

    final_qps = mean_qps / AVG_CALC
    mean_closed_loop = mean_closed_loop / AVG_CALC
    print(
        f"Throughput QPS: {final_qps}\n"
        f"A Replicas: {NUM_REPLICAS_A}, B Replicas: {NUM_REPLICAS_B}\n"
        f"Mean Closed Loop Latency: {mean_closed_loop} "
    )
    print(ray.get(serve_handle.remote(args=1)))
    ray.shutdown()

if __name__ == "__main__":
    driver()
