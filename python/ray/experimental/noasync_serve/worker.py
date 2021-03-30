import ray

class Worker:
    '''
    Base Worker class to override. NOTE this is not a ray actor, so deriving class
    must use `ray.remote()` decorator
    '''
    def __init__(self, name, router_handle, func):
        self.name = name
        self._router_handle = router_handle
        self.func = func
        self._my_handle = None

    def __call__(self, requests):
        '''
        Calls self.func on requests passed by router, and then rejoins router queue
        '''
        for request in requests:
            # get params
            request_result_object_id = request.result_object_id
            request_body = request.request_body

            # execute request
            output = self.func(request_body)

            # put output into result_id
            ray.put(output, request_result_object_id)
            #print("finished task and put result into OID")

        # join router again
        self._router_handle.dequeue_request.remote(self.name, self._my_handle)

    def set_my_handle(self, my_handle):
        '''
        Saves worker's own handle
        '''
        self._my_handle = my_handle

    def start(self):
        '''
        Makes the worker join the router queue
        '''
        self._router_handle.dequeue_request.remote(self.name, self._my_handle)

@ray.remote
class ImagePreprocWorker(Worker):
    '''
    Single worker that converts raw image to tensor and then predicts its class
    '''
    def __init__(self, name, router_handle, transform, model_name, is_cuda=False):
        # imports needed
        import io
        import torch
        import warnings
        from PIL import Image
        from torchvision import models
        from torch.autograd import Variable

        # initialize fields
        super().__init__(name, router_handle, None)
        self.transform = transform
        self.model = models.__dict__[model_name](pretrained=True)
        self.is_cuda = is_cuda
        warnings.filterwarnings("ignore")
        if is_cuda:
            self.model = self.model.cuda()

        # set self.func
        def call(request):
            data = Image.open(io.BytesIO(request))
            if data.mode != "RGB":
                data = data.convert("RGB")
            data = self.transform(data)

            data = torch.stack([data])
            data = Variable(data)
            if self.is_cuda:
                 data = data.cuda()
            outputs = self.model(data)
            _, predicted = outputs.max(1)

            return predicted.cpu().numpy().tolist()[0]

        self.func = call
