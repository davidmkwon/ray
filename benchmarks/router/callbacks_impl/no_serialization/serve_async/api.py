import ray
import inspect
from functools import wraps

from queues import DequeRouter
from handle import RayServeHandle
from backend_config import BackendConfig
from constants import (
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
    SERVE_MASTER_NAME,
)
from global_state import GlobalState, start_initial_state
from utils import get_random_letters

# TODO: remove router_handle
global_state = None

def _get_global_state():
    """Used for internal purpose. Because just import serve.global_state
    will always reference the original None object
    """
    return global_state

def _ensure_connected(f):
    @wraps(f)
    def check(*args, **kwargs):
        if _get_global_state() is None:
            raise RayServeException(
                "Please run serve.init to initialize or "
                "connect to existing ray serve cluster."
            )
        return f(*args, **kwargs)

    return check

def init(ray_init_kwargs, uvloop=True):
    # exit if already initialized
    global global_state
    if global_state is not None:
        return

    # initialize ray if needed
    if not ray.is_initialized():
        ray.init(**ray_init_kwargs)

    # Try to get serve master actor if it exists
    try:
        master_actor_handle = ray.get_actor(SERVE_MASTER_NAME)
        global_state = GlobalState(master_actor_handle=master_actor_handle)
        return
    except ValueError:
        pass

    # for now we will use a in-memory simple KV store
    from kv_store_service import InMemoryKVStore
    master = start_initial_state(
        kv_store_connector=lambda namespace: InMemoryKVStore(namespace)
    )
    global_state = GlobalState(master)

def set_pipeline(pipeline_dict, source):
    router_handle = global_state.init_or_get_router()
    ray.get(router_handle.set_pipeline.remote(pipeline_dict))
    ray.get(router_handle.set_source.remote(source))

@_ensure_connected
def _create_backend(func_or_class, backend_tag, backend_config=None):
    # default backend_config
    if backend_config is None:
        backend_config = BackendConfig()
    assert isinstance(backend_config, BackendConfig), (
        "backend_config must be" " of instance BackendConfig"
    )

    # make actor "creator" functions for spawning later
    if inspect.isfunction(func_or_class):
        @ray.remote
        class RayFunc:
            def __call__(self, args):
                return func_or_class(args)

        # default to 1 replica is no BackendConfig?
        #num_replicas = backend_config.num_replicas
        #replica_objectrefs = [
        #    router_handle.dequeue_request.remote(endpoint_name, RayFunc.remote())
        #    for _ in range(num_replicas)
        #]
        #ray.get(replica_objectrefs)
        creator = lambda : RayFunc.remote()
    elif inspect.isclass(func_or_class):
        @ray.remote
        class RayClass(func_or_class):
            pass

        # default to 1 replica is no BackendConfig?
        #replica_objectrefs = [
        #    router_handle.dequeue_request.remote(endpoint_name, RayClass.remote())
        #    for _ in range(num_replicas)
        #]
        #ray.get(replica_objectrefs)
        creator = lambda : RayClass.remote()
    else:
        raise TypeError(
            "Backend must be a function or class, it is {}.".format(
                type(func_or_class)
            )
        )

    # save creator which starts replicas + replica config metadata and init args
    backend_config_dict = dict(backend_config)
    global_state.backend_table.register_backend(backend_tag, creator)
    global_state.backend_table.register_info(backend_tag, backend_config_dict)
    global_state.backend_table.save_init_args(backend_tag, arg_list=())

    # set the backend config inside the router
    #ray.get(
    #    global_state.init_or_get_router().set_backend_config.remote(
    #        backend_tag, backend_config_dict
    #    )
    #)
    _scale(backend_tag, backend_config_dict["num_replicas"])

@_ensure_connected
def _scale(backend_tag, num_replicas):
    assert (
        backend_tag in global_state.backend_table.list_backends()
    ), "Backend {} is not registered.".format(backend_tag)
    assert num_replicas >= 0, (
        "Number of replicas must be" " greater than or equal to 0."
    )

    replicas = global_state.backend_table.list_replicas(backend_tag)
    current_num_replicas = len(replicas)
    delta_num_replicas = num_replicas - current_num_replicas

    if delta_num_replicas > 0:
        for _ in range(delta_num_replicas):
            _start_replica(backend_tag)
    elif delta_num_replicas < 0:
        for _ in range(-delta_num_replicas):
            _remove_replica(backend_tag)

def _start_replica(backend_tag):
    assert (
        backend_tag in global_state.backend_table.list_backends()
    ), "Backend {} is not registered.".format(backend_tag)

    replica_tag = "worker:{}#{}".format(
        backend_tag, get_random_letters(length=6)
    )

    # get the info which starts the replicas
    creator = global_state.backend_table.get_backend_creator(backend_tag)
    backend_config_dict = global_state.backend_table.get_info(backend_tag)
    backend_config = BackendConfig(**backend_config_dict)
    init_args = global_state.backend_table.get_init_args(backend_tag)

    # get actor creation kwargs
    actor_kwargs = backend_config.get_actor_creation_args(init_args)

    # Create the worker in the master actor
    [worker_handle] = ray.get(
        global_state.master_actor_handle.start_actor_with_creator.remote(
            creator, actor_kwargs, replica_tag
        )
    )

    # add the worker to the router queue
    router_handle = global_state.init_or_get_router()
    router_handle.dequeue_request.remote(backend_tag, worker_handle)

    # add worker replica tag to KV store
    global_state.backend_table.add_replica(backend_tag, replica_tag)

def _remove_replica(backend_tag):
    assert (
        backend_tag in global_state.backend_table.list_backends()
    ), "Backend {} is not registered.".format(backend_tag)
    assert (
        len(global_state.backend_table.list_replicas(backend_tag)) > 0
    ), "Backend {} does not have enough replicas to be removed.".format(
        backend_tag
    )
    # get corresponding replica tag from master actor
    replica_tag = global_state.backend_table.remove_replica(backend_tag)
    [replica_handle] = ray.get(
        global_state.master_actor_handle.get_handle.remote(replica_tag)
    )

    # Remove the replica from master actor.
    ray.get(global_state.master_actor_handle.remove_handle.remote(replica_tag))

    # Remove the replica from router.
    # This will also destory the actor handle.
    ray.get(
        global_state.init_or_get_router().remove_and_destory_replica.remote(
            backend_tag, replica_handle
        )
    )

@_ensure_connected
def create_endpoint(func_or_class, backend_tag, backend_config=None):
    _create_backend(func_or_class, backend_tag, backend_config)

@_ensure_connected
def get_backend_config(backend_tag):
    assert (
        backend_tag in global_state.backend_table.list_backends()
    ), "Backend {} is not registered.".format(backend_tag)
    backend_config_dict = global_state.backend_table.get_info(backend_tag)
    return BackendConfig(**backend_config_dict)

@_ensure_connected
def set_backend_config(backend_tag, backend_config):
    assert (
        backend_tag in global_state.backend_table.list_backends()
    ), "Backend {} is not registered.".format(backend_tag)
    assert isinstance(backend_config, BackendConfig), (
        "backend_config must be" " of instance BackendConfig"
    )
    backend_config_dict = dict(backend_config)
    old_backend_config_dict = global_state.backend_table.get_info(backend_tag)

    global_state.backend_table.register_info(backend_tag, backend_config_dict)

    # (BATCH) inform the router about change in configuration
    # particularly for setting max_batch_size
    #ray.get(
    #    global_state.init_or_get_router().set_backend_config.remote(
    #        backend_tag, backend_config_dict
    #    )
    #)

    # checking if replicas need to be restarted
    # Replicas are restarted if there is any change in the backend config
    # related to restart_configs
    # TODO(alind) : have replica restarting policies selected by the user

    need_to_restart_replicas = any(
        old_backend_config_dict[k] != backend_config_dict[k]
        for k in BackendConfig.restart_on_change_fields
    )
    if need_to_restart_replicas:
        # kill all the replicas for restarting with new configurations
        _scale(backend_tag, 0)

    # scale the replicas with new configuration
    _scale(backend_tag, backend_config_dict["num_replicas"])

def using_router(router_name="default"):
    return global_state.using_router(router_name)

@_ensure_connected
def get_handle():
    router_handle = global_state.init_or_get_router()
    source = ray.get(router_handle.get_source.remote())
    return RayServeHandle(router_handle, source)
