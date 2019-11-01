import time

import requests
from werkzeug import urls
import ray
from ray.experimental import serve
from ray.experimental.serve.utils import pformat_color_json
import json
from ray.experimental.serve.utils import BytesEncoder
from torchvision.models.resnet import resnet50
import io
from PIL import Image
from torch.autograd import Variable
import torchvision.transforms as transforms
import base64
from pprint import pprint
import torch
from ray.experimental import async_api
import asyncio
import functools

class Transform:
	def __init__(self,transform):
		self.transform = transform
	def __call__(self,batch_data):
		batch_size = len(batch_data)
		result = []
		for i in range(batch_size):
			data = Image.open(io.BytesIO(base64.b64decode(batch_data[i])))
			if data.mode != "RGB":
				data = data.convert("RGB")
			data = self.transform(data)
			# data = data.unsqueeze(0)
			result.append(data)
		return result

class Resnet50:
	def __init__(self, model):
		self.model = model

	def __call__(self, batch_data):
		# if 'transform' in context:
		# data = context['transform']
		data = torch.stack(batch_data)
		data = Variable(data)
		data = data.cuda()
		outputs = self.model(data)
		_, predicted = outputs.max(1)
		return predicted.cpu().numpy().tolist()


min_img_size = 224
transform = transforms.Compose([transforms.Resize(min_img_size),
                                         transforms.ToTensor(),
                                         transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                                              std=[0.229, 0.224, 0.225])])
model = resnet50(pretrained=True)
model = model.cuda()

serve.init(object_store_memory=int(1e9),blocking=True)
#create Backends
serve.create_backend(Transform, "transform:v1",0,transform)
serve.create_backend(Resnet50,"r50",1,model)

# create service
serve.create_no_http_service("transform",max_batch_size=3)
serve.create_no_http_service("imagenet-classification",max_batch_size=8)

#link service and backend
serve.link_service("transform", "transform:v1")
serve.link_service("imagenet-classification", "r50")

serve.add_service_dependencies("pipeline1","transform","imagenet-classification")

# Provision the PIPELINE (You can provision the pipeline only once)
serve.provision_pipeline("pipeline1")


dependency = serve.get_service_dependencies("pipeline1")
pipeline_handle = serve.get_handle("pipeline1")

future_list = []
def result_callback(future, d):
	d[future] = time.time()

loop = asyncio.get_event_loop()
start_time_d = {}
end_time_d = {}
num_q = 40

# import concurrent.futures 
# executor = concurrent.futures.ThreadPoolExecutor(max_workers=8) 

async def fire_queries(start_time_d,end_time_d,num_q):
	await async_api._async_init()
	futures = set()
	for r in range(num_q):
		req_json = { "transform": base64.b64encode(open('elephant.jpg', "rb").read()) }
		start_time = time.time()
		f = async_api.as_future(pipeline_handle.remote(**req_json))
		f.add_done_callback(functools.partial(result_callback, d=end_time_d))
		start_time_d[f] = start_time
		futures.add(f)

	done, pending = await asyncio.wait(futures)
	return

loop.run_until_complete(fire_queries(start_time_d,end_time_d,num_q))

for f in start_time_d.keys():
	time_taken = end_time_d[f] - start_time_d[f]
	print("Time taken : {}".format(time_taken))



	# future_list.append(f)
# results = ray.get(future_list)
# for result in results:
# 	print("-----------------------------")
# 	print(result)

