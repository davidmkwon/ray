import time

import requests
from werkzeug import urls

from ray.experimental import serve
from ray.experimental.serve.utils import pformat_color_json
import json
from pprint import pprint
import ray
def echo1(*context):
	batch_size = len(context[0])
	result = []
	for i in range(batch_size):
		start = "[ "
		for val in context:
			start =  start + val[i] + " , "
			# message += ' FROM MODEL1 -> '
		start += " ] --> "
		start += 'FROM MODEL1/{} -> '.format(batch_size)
		result.append(start)
	return result
def echo2(*context):
	batch_size = len(context[0])
	result = []
	for i in range(batch_size):
		start = "[ "
		for val in context:
			start =  start + val[i] + " , "
			# message += ' FROM MODEL1 -> '
		start += " ] --> "
		start += 'FROM MODEL2/{} -> '.format(batch_size)
		result.append(start)
	return result

def echo3(*context):
	batch_size = len(context[0])
	result = []
	for i in range(batch_size):
		start = "[ "
		for val in context:
			start =  start + val[i] + " , "
			# message += ' FROM MODEL1 -> '
		start += " ] --> "
		start += 'FROM MODEL3/{} -> '.format(batch_size)
		result.append(start)
	return result

serve.init(blocking=True)

# serve.create_endpoint_pipeline("pipeline1", "/echo", blocking=True)

# Create Backends
serve.create_backend(echo1, "echo:v1",num_gpu=0)
serve.create_backend(echo2, "echo:v2",num_gpu=0)
serve.create_backend(echo3,"echo:v3",num_gpu=0)

# Create services
serve.create_no_http_service("serve1",max_batch_size=5)
serve.create_no_http_service("serve2",max_batch_size=1)
serve.create_no_http_service("serve3",max_batch_size=2)

# Link services and backends
serve.link_service("serve1", "echo:v1")
serve.link_service("serve2", "echo:v2")
serve.link_service("serve3","echo:v3")

'''
1. Add service dependencies in a PIPELINE
2. You can add dependency to a PIPELINE only if the PIPELINE has not been provisioned yet.
'''
'''
Creating a pipeline [serve1 , serve2] -> serve3
'''
# serve3 depends on serve1
serve.add_service_dependencies("pipeline1","serve1","serve3")
# serve3 depends on serve2
serve.add_service_dependencies("pipeline1","serve2","serve3")

# Provision the PIPELINE (You can provision the pipeline only once)
serve.provision_pipeline("pipeline1")

# You can only create an endpoint for pipeline after provisioning the pipeline
# serve.create_endpoint_pipeline("pipeline1", "/echo", blocking=True)
dependency = serve.get_service_dependencies("pipeline1")
pprint(dependency)
node_list = dependency['node_order'][0]
future_list = []
pipeline_handle = serve.get_handle("pipeline1")
for r in range(8):
	sent = {}
	for n in node_list:
		sent[n] = "INP-{}".format(r)
	sent['slo'] = 100 + r
	f = pipeline_handle.remote(**sent)
	future_list.append(f)

# results = ray.get(future_list)
results = ray.get(future_list)
for result in results:
	print("--------------------------------")
	print(result)
