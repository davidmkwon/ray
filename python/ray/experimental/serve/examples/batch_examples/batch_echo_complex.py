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
	

def echo4(*context):
	batch_size = len(context[0])
	result = []
	for i in range(batch_size):
		start = "[ "
		for val in context:
			start =  start + val[i] + " , "
			# message += ' FROM MODEL1 -> '
		start += " ] --> "
		start += 'FROM MODEL4/{} -> '.format(batch_size)
		result.append(start)
	return result
	

def echo5(*context):
	batch_size = len(context[0])
	result = []
	for i in range(batch_size):
		start = "[ "
		for val in context:
			start =  start + val[i] + " , "
			# message += ' FROM MODEL1 -> '
		start += " ] --> "
		start += 'FROM MODEL5/{} -> '.format(batch_size)
		result.append(start)
	return result
	
def echo6(*context):
	batch_size = len(context[0])
	result = []
	for i in range(batch_size):
		start = "[ "
		for val in context:
			start =  start + val[i] + " , "
			# message += ' FROM MODEL1 -> '
		start += " ] --> "
		start += 'FROM MODEL6/{} -> '.format(batch_size)
		result.append(start)
	return result
	

def echo7(*context):
	batch_size = len(context[0])
	result = []
	for i in range(batch_size):
		start = "[ "
		for val in context:
			start =  start + val[i] + " , "
			# message += ' FROM MODEL1 -> '
		start += " ] --> "
		start += 'FROM MODEL7/{} -> '.format(batch_size)
		result.append(start)
	return result
	

def echo8(*context):
	batch_size = len(context[0])
	result = []
	for i in range(batch_size):
		start = "[ "
		for val in context:
			start =  start + val[i] + " , "
			# message += ' FROM MODEL1 -> '
		start += " ] --> "
		start += 'FROM MODEL8/{} -> '.format(batch_size)
		result.append(start)
	return result
	

def echo9(*context):
	batch_size = len(context[0])
	result = []
	for i in range(batch_size):
		start = "[ "
		for val in context:
			start =  start + val[i] + " , "
			# message += ' FROM MODEL1 -> '
		start += " ] --> "
		start += 'FROM MODEL9/{} -> '.format(batch_size)
		result.append(start)
	return result
	

def echo10(*context):
	batch_size = len(context[0])
	result = []
	for i in range(batch_size):
		start = "[ "
		for val in context:
			start =  start + val[i] + " , "
			# message += ' FROM MODEL1 -> '
		start += " ] --> "
		start += 'FROM MODEL10/{} -> '.format(batch_size)
		result.append(start)
	return result
	

serve.init(blocking=True)

# serve.create_endpoint_pipeline("pipeline1", "/echo", blocking=True)

# Create Backends
serve.create_backend(echo1, "echo:v1",num_gpu=0)
serve.create_backend(echo2, "echo:v2",num_gpu=0)
serve.create_backend(echo3,"echo:v3",num_gpu=0)
serve.create_backend(echo4,"echo:v4",num_gpu=0)
serve.create_backend(echo5,"echo:v5",num_gpu=0)
serve.create_backend(echo6,"echo:v6",num_gpu=0)
serve.create_backend(echo7,"echo:v7",num_gpu=0)
serve.create_backend(echo8,"echo:v8",num_gpu=0)
serve.create_backend(echo9,"echo:v9",num_gpu=0)
serve.create_backend(echo10,"echo:v10",num_gpu=0)

# Create services
serve.create_no_http_service("serve1",max_batch_size=5)
serve.create_no_http_service("serve2",max_batch_size=5)
serve.create_no_http_service("serve3",max_batch_size=5)
serve.create_no_http_service("serve4",max_batch_size=2)
serve.create_no_http_service("serve5",max_batch_size=1)
serve.create_no_http_service("serve6",max_batch_size=5)
serve.create_no_http_service("serve7",max_batch_size=9)
serve.create_no_http_service("serve8",max_batch_size=10)
serve.create_no_http_service("serve9",max_batch_size=3)
serve.create_no_http_service("serve10",max_batch_size=7)
# serve.create_no_http_service("serve3")

# Link services and backends
serve.link_service("serve1", "echo:v1")
serve.link_service("serve2", "echo:v2")
serve.link_service("serve3","echo:v3")
serve.link_service("serve4","echo:v4")
serve.link_service("serve5","echo:v5")
serve.link_service("serve6","echo:v6")
serve.link_service("serve7","echo:v7")
serve.link_service("serve8","echo:v8")
serve.link_service("serve9","echo:v9")
serve.link_service("serve10","echo:v10")

'''
1. Add service dependencies in a PIPELINE
2. You can add dependency to a PIPELINE only if the PIPELINE has not been provisioned yet.
'''

# Adding dependencies
serve.add_service_dependencies("pipeline1","serve1","serve2")
serve.add_service_dependencies("pipeline1","serve1","serve3")
serve.add_service_dependencies("pipeline1","serve1","serve5")

serve.add_service_dependencies("pipeline1","serve2","serve4")
serve.add_service_dependencies("pipeline1","serve2","serve5")

serve.add_service_dependencies("pipeline1","serve3","serve6")
serve.add_service_dependencies("pipeline1","serve3","serve7")

serve.add_service_dependencies("pipeline1","serve4","serve8")

serve.add_service_dependencies("pipeline1","serve5","serve9")

serve.add_service_dependencies("pipeline1","serve6","serve5")
serve.add_service_dependencies("pipeline1","serve6","serve8")

serve.add_service_dependencies("pipeline1","serve7","serve4")
serve.add_service_dependencies("pipeline1","serve7","serve9")

serve.add_service_dependencies("pipeline1","serve8","serve10")

serve.add_service_dependencies("pipeline1","serve9","serve10")







# Provision the PIPELINE (You can provision the pipeline only once)
serve.provision_pipeline("pipeline1")
dependency = serve.get_service_dependencies("pipeline1")
pprint(dependency)
node_list = dependency['node_order'][0]
future_list = []
pipeline_handle = serve.get_handle("pipeline1")
for r in range(12):
	sent = {}
	for n in node_list:
		sent[n] = "INP-{}".format(r)
	sent['slo'] = 100 + r
	f = pipeline_handle.remote(**sent)
	future_list.append(f)
results = ray.get(future_list)
for result in results:
	print("--------------------------------")
	print(result)

# # You can only create an endpoint for pipeline after provisioning the pipeline
# serve.create_endpoint_pipeline("pipeline1", "/echo", blocking=True)

# time.sleep(2)

# while True:
#     resp = requests.get("http://127.0.0.1:8000/echo").json()
#     print(pformat_color_json(resp))

#     print("...Sleeping for 2 seconds...")
#     time.sleep(2)