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


def query():
	d = {
	'index': '',
	'start_time': '',
	'end_time': '',
	'slo': '' ,
	'data': ''
	    }
	return d

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
query_stats = {}
query_list = []

for r in range(40):
	q = query()
	q['slo'] = 70
	q['index'] = r
	req_json = { "transform": base64.b64encode(open('../elephant.jpg', "rb").read()) }
	req_json['slo'] = q['slo']
	q['data'] = req_json
	query_list.append(q)
for q in query_list:
		# q['start_time'] = time.time()
		f = pipeline_handle.remote(**q['data'])
		future_list.append(f)
		# query_stats[f] = q
# for r in range(12):
# 	req_json = { "transform": base64.b64encode(open('elephant.jpg', "rb").read()) }
# 	q = {'start_time' : time.time()}
# 	f = pipeline_handle.remote(**req_json)
# 	q['index'] = r
# 	query_stats[]
# 	future_list.append(f)

start = time.time()
results = ray.get(future_list)
end = time.time()
print("Time taken: {}".format((end - start)))
for result in results:
	print("-----------------------------")
	print(result)
# left_futures = future_list
# while left_futures:
# 	completed_futures , remaining_futures = ray.wait(left_futures,timeout=0.05)
# 	if len(completed_futures) > 0:
# 		end_time = time.time()
# 		f = completed_futures[0]
# 		query_stats[f]['end_time'] = end_time
		
# 		# result = ray.get(f)
# 		# print("--------------------------------")
# 		# print(result)
# 	left_futures = remaining_futures


# for f in query_stats.keys():
# 	val = query_stats[f]
# 	time_taken = val['end_time'] - val['start_time']
# 	print("Query: {} Time Taken: {}".format(val['index'],time_taken))

