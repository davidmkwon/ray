import time

from werkzeug import urls
from threading import Thread
from ray.experimental import serve
from ray.experimental.serve.utils import pformat_color_json
import json
import requests
import ray
# import grequests as async
def echo1(context):
	result = []
	batch_size = len(context)
	time.sleep(5)
	for i in range(batch_size):
		message = ""
		message += 'FROM MODEL1/BS-{} -> '.format(batch_size)
		result.append(message)
	return result
def echo2(context):
	result = []
	batch_size = len(context)
	time.sleep(5)
	for i in range(batch_size):
		message = context[i]
		message += 'FROM MODEL2/BS-{} -> '.format(batch_size)
		result.append(message)
	return result

def echo3(context):
	result = []
	batch_size = len(context)
	time.sleep(5)
	for i in range(batch_size):
		message = context[i]
		message += 'FROM MODEL3/BS-{} -> '.format(batch_size)
		result.append(message)
	return result

serve.init(blocking=True)

# serve.create_endpoint_pipeline("pipeline1", "/echo", blocking=True)

# Create Backends
serve.create_backend(echo1, "echo:v1",num_gpu=0)
serve.create_backend(echo2, "echo:v2",num_gpu=0)
serve.create_backend(echo3,"echo:v3",num_gpu=0)

# Create services
serve.create_no_http_service("serve1",max_batch_size=5)
serve.create_no_http_service("serve2",max_batch_size=4)
serve.create_no_http_service("serve3",max_batch_size=3)

# Link services and backends
serve.link_service("serve1", "echo:v1")
serve.link_service("serve2", "echo:v2")
serve.link_service("serve3","echo:v3")

'''
1. Add service dependencies in a PIPELINE
2. You can add dependency to a PIPELINE only if the PIPELINE has not been provisioned yet.
'''
'''
Creating a pipeline serve1 -> serve2 -> serve3
'''
# serve2 depends on serve1
serve.add_service_dependencies("pipeline1","serve1","serve2")
# serve3 depends on serve2
serve.add_service_dependencies("pipeline1","serve2","serve3")

# Provision the PIPELINE (You can provision the pipeline only once)
serve.provision_pipeline("pipeline1")

# You can only create an endpoint for pipeline after provisioning the pipeline
# serve.create_endpoint_pipeline("pipeline1", "/echo", blocking=True)
pipeline_handle = serve.get_handle("pipeline1")
args = {"serve1": "Intial Data --> "}
futures = [pipeline_handle.remote(**args) for i in range(8)]
results = ray.get(futures)
for result in results:
	print("------------------------------")
	print(result)
# time.sleep(2)


# while True:
#     resp = requests.get("http://127.0.0.1:8000/echo").json()
#     print(pformat_color_json(resp))


#     print("...Sleeping for 2 seconds...")
#     time.sleep(1)