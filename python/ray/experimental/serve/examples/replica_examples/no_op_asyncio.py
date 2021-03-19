import ray
import queue
import time
import asyncio
import functools
from ray.experimental import async_api

@ray.remote
def simple_func():
    return 1

def result_callback(future, d):
	d[future] = time.time()

async def fire_queries(start_time_d,end_time_d,num_q):
	await async_api._async_init()
	futures = set()
	for r in range(num_q):
		start_time = time.time()
		f = async_api.as_future(simple_func.remote())
		f.add_done_callback(functools.partial(result_callback, d=end_time_d))
		start_time_d[f] = start_time
		futures.add(f)
	done, pending = await asyncio.wait(futures)
	return


ray.init()
loop = asyncio.get_event_loop()
start_time_d = {}
end_time_d = {}
num_q = 1000
loop.run_until_complete(fire_queries(start_time_d,end_time_d,num_q))
for f in start_time_d.keys():
	time_taken = end_time_d[f] - start_time_d[f]
	print("OID: {} Time Taken (in seconds): {}".format(f,time_taken))
