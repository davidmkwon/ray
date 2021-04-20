import ray
import inspect

from queues import DequeRouter
from handle import RayServeHandle
from backend_config import BackendConfig


# (not good) global handle to router actor
router_handle = None

def init(
    ray_init_kwargs,
    uvloop=True
):
    # initialize ray if needed
    if not ray.is_initialized():
        ray.init(**ray_init_kwargs)

    # BAD: for now just have a global variable router_handle
    global router_handle
    router_handle = DequeRouter.remote(uvloop)

def set_pipeline(pipeline_dict, source):
    ray.get(router_handle.set_pipeline.remote(pipeline_dict))
    ray.get(router_handle.set_source.remote(source))

def create_endpoint(
    func_or_class, endpoint_name, backend_config=None
):
    if inspect.isfunction(func_or_class):
        @ray.remote
        class RayFunc:
            def __call__(self, args):
                return func_or_class(args)

        # default to 1 replica is no BackendConfig?
        num_replicas = 1 if backend_config is None else backend_config.num_replicas
        replica_objectrefs = [
            router_handle.dequeue_request.remote(endpoint_name, RayFunc.remote())
            for _ in range(num_replicas)
        ]
        ray.get(replica_objectrefs)
    elif inspect.isclass(func_or_class):
        @ray.remote
        class RayClass(func_or_class):
            pass

        # default to 1 replica is no BackendConfig?
        replica_objectrefs = [
            router_handle.dequeue_request.remote(endpoint_name, RayClass.remote())
            for _ in range(num_replicas)
        ]
        ray.get(replica_objectrefs)
    else:
        raise TypeError(
            "Backend must be a function or class, it is {}.".format(
                type(func_or_class)
            )
        )

def get_handle():
    source = ray.get(router_handle.get_source.remote())
    return RayServeHandle(router_handle, source)
