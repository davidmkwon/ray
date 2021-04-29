import numpy as np


def gamma(mean, cv, size):
    if cv == 0.0:
        return np.ones(size) * mean
    else:
        return np.random.gamma(1.0 / cv, cv * mean, size=size)


def generate_fixed_arrival_process(mean_qps, cv, num_requests):
    """
    mean_qps : float
        Mean qps
    cv : float
    duration: float
        Duration of the trace in seconds
    """
    # deltas_path = os.path.join(arrival_process_dir,
    #                            "fixed_{mean_qps}_{cv}_{dur}_{ts:%y%m%d_%H%M%S}.deltas".format(
    #                                mean_qps=mean_qps, cv=cv, dur=duration, ts=datetime.now()))
    inter_request_delay_ms = 1.0 / float(mean_qps) * 1000.0
    num_deltas = num_requests - 1
    if cv == 0:
        deltas = np.ones(num_deltas) * inter_request_delay_ms
    else:
        deltas = gamma(inter_request_delay_ms, cv, size=num_deltas)
    deltas = np.clip(deltas, a_min=2.5, a_max=None)
    return deltas


def _get_ingest_observed_throughput(start_time_list):
    start_time_list.sort()
    avg_time_diff = 0
    cnt = 0
    for i in range(len(start_time_list) - 1):
        avg_time_diff += start_time_list[i + 1] - start_time_list[i]
        cnt += 1
    avg_time_diff = avg_time_diff / cnt
    return 1.0 / avg_time_diff


def get_latency_stats(collected_latency):

    latency_list_ms = [
        (d["end"] - d["start"]) * 1000 for d in collected_latency
    ]
    p95_ms, p99_ms = np.percentile(latency_list_ms, [95, 99])

    ingest_throughput = _get_ingest_observed_throughput(
        [d["start"] for d in collected_latency]
    )
    return ingest_throughput, latency_list_ms, p95_ms, p99_ms