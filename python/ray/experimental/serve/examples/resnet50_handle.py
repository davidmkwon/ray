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
class Transform:
	def __init__(self,transform):
		self.transform = transform
	def __call__(self,data):
		data = Image.open(io.BytesIO(base64.b64decode(data[0])))
		if data.mode != "RGB":
			data = data.convert("RGB")
		data = self.transform(data)
		data = data.unsqueeze(0)
		return [data]

class Resnet50:
	def __init__(self, model):
		self.model = model

	def __call__(self, data):
		# if 'transform' in context:
		# data = context['transform']
		data = Variable(data[0])
		data = data.cuda()
		return [self.model(data).data.cpu().numpy().argmax()]
		# return context['transform']
		# return ''



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
serve.create_no_http_service("transform")
serve.create_no_http_service("imagenet-classification")

#link service and backend
serve.link_service("transform", "transform:v1")
serve.link_service("imagenet-classification", "r50")

#Add service dependencies to pipeline
serve.add_service_dependencies("pipeline1","transform","imagenet-classification")

# Provision the PIPELINE (You can provision the pipeline only once)
serve.provision_pipeline("pipeline1")


dependency = serve.get_service_dependencies("pipeline1")
pipeline_handle = serve.get_handle("pipeline1")
req_json = { "transform": base64.b64encode(open('elephant.jpg', "rb").read()) }
# sent_data = json.dumps(req_json, cls=BytesEncoder, indent=2).encode()
result = pipeline_handle.remote(**req_json)
result = ray.get(result)
print(result)
