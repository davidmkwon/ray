import time

import requests
from werkzeug import urls

from ray.experimental import serve
from ray.experimental.serve.utils import pformat_color_json
import json
from pprint import pprint
import ray
def echo1(context):
	message = context[0]
	message += 'FROM MODEL1 -> '
	return [message]


serve.init(blocking=True)
serve.create_backend(echo1, "echo:v1",num_gpu=0)
serve.create_no_http_service("serve1")
serve.link_service("serve1", "echo:v1")

serve.add_service("pipeline1","serve1")
serve.provision_pipeline("pipeline1")
pprint(serve.get_service_dependencies("pipeline1"))
pipeline_handle = serve.get_handle("pipeline1")
args = {"serve1": "Intial Data --> "}
result = pipeline_handle.remote(**args)
result = ray.get(result)
print("Result is")
print(result)