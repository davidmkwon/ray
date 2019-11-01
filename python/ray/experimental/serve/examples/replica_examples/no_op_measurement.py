import ray
import queue
import time

@ray.remote
def simple_func():
    return 1



def examine_futures(future_queue,timing_stats,num_q):
	pending_futures = []
	print("Started")
	c = 0
	while True:
		new_pending_futures = []	
		try:
		    item  = future_queue.get(block=True,timeout=0.0009)
		    new_pending_futures.append(item)
		    c += 1
		except Exception:
		    pass
		pending_futures = pending_futures + new_pending_futures
		if len(pending_futures) == 0 and c == num_q:
			break
		if len(pending_futures) == 0:
			continue
		completed_futures , remaining_futures = ray.wait(pending_futures,timeout=0.0001)
		if len(completed_futures) == 1:
			f = completed_futures[0]
			timing_stats[f] = time.time()
		pending_futures = remaining_futures
	print("ended")
	return



def send_queries(future_queue,associated_query,num_q):
	for i in range(num_q):
		# print("here")
		start_time = time.time()
		f = simple_func.remote()
		future_queue.put_nowait(f)
		associated_query[f] = start_time
		# print("completed")
	print("Submitted: {}".format(i))
	# print("")

ray.init()
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
associated_query = {}
timing_stats = {}
num_q = 1000

future_queue = queue.Queue()
pool = ThreadPoolExecutor(2)
f1 = pool.submit(send_queries,future_queue,associated_query,num_q)
f2 = pool.submit(examine_futures,future_queue,timing_stats,num_q)
wait([f1,f2])

for f in associated_query.keys():
	time_taken = timing_stats[f] - associated_query[f]
	print("OID: {}  Time Taken (in seconds): {}".format(f,time_taken))

