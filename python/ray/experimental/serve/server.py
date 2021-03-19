import asyncio
import json

import time
import os

import uvicorn
from collections import defaultdict, deque
import ray
from ray.experimental.async_api import _async_init, as_future
from ray.experimental.serve.utils import BytesEncoder
from ray.experimental.serve.constants import HTTP_ROUTER_CHECKER_INTERVAL_S
from networkx.readwrite import json_graph
class JSONResponse:
    """ASGI compliant response class.

    It is expected to be called in async context and pass along
    `scope, receive, send` as in ASGI spec.

    >>> await JSONResponse({"k": "v"})(scope, receive, send)
    """

    def __init__(self, content=None, status_code=200):
        """Construct a JSON HTTP Response.

        Args:
            content (optional): Any JSON serializable object.
            status_code (int, optional): Default status code is 200.
        """
        self.body = self.render(content)
        self.status_code = status_code
        self.raw_headers = [[b"content-type", b"application/json"]]

    def render(self, content):
        if content is None:
            return b""
        if isinstance(content, bytes):
            return content
        return json.dumps(content, cls=BytesEncoder, indent=2).encode()

    async def __call__(self, scope, receive, send):
        await send({
            "type": "http.response.start",
            "status": self.status_code,
            "headers": self.raw_headers,
        })
        await send({"type": "http.response.body", "body": self.body})


class HTTPProxy:
    """
    This class should be instantiated and ran by ASGI server.

    >>> import uvicorn
    >>> uvicorn.run(HTTPProxy(kv_store_actor_handle, router_handle))
    # blocks forever
    """

    def __init__(self, kv_store_actor_handle,kv_store_actor_pipeline_handle ,router_handle):
        """
        Args:
            kv_store_actor_handle (ray.actor.ActorHandle): handle to routing
               table actor. It will be used to populate routing table. It
               should implement `handle.list_service()`
            router_handle (ray.actor.ActorHandle): actor handle to push request
               to. It should implement
               `handle.enqueue_request.remote(endpoint, body)`
        """
        assert ray.is_initialized()

        self.admin_actor = kv_store_actor_handle
        self.pipeline_actor = kv_store_actor_pipeline_handle
        self.router = router_handle
        self.route_table = dict()
        self.pipeline_table = dict()
        self.profile_file = open(os.environ.get("SERVE_PROFILE_PATH", "/tmp/serve_profile.jsonl"),'w')

    async def route_checker(self, interval):
        while True:
            try:
                self.route_table = await as_future(
                    self.admin_actor.list_service.remote())
                self.pipeline_table = await as_future(
                    self.pipeline_actor.list_pipeline_service.remote())
            except ray.exceptions.RayletError:  # Gracefully handle termination
                return

            await asyncio.sleep(interval)
    async def read_body(self,receive):
        """
        Read and return the entire body from an incoming ASGI message.
        """
        body = b''
        more_body = True

        while more_body:
            message = await receive()
            body += message.get('body', b'')
            more_body = message.get('more_body', False)

        return json.loads(body)

    async def __call__(self, scope, receive, send):
        # NOTE: This implements ASGI protocol specified in
        #       https://asgi.readthedocs.io/en/latest/specs/index.html

        if scope["type"] == "lifespan":
            await _async_init()
            asyncio.ensure_future(
                self.route_checker(interval=HTTP_ROUTER_CHECKER_INTERVAL_S))
            return

        current_path = scope["path"]
        if current_path == "/":
            await JSONResponse(self.route_table)(scope, receive, send)
        elif current_path in self.route_table:
            pipeline_name = self.route_table[current_path]
            if scope['method'] == 'GET' :
                service_dependencies = self.pipeline_table[pipeline_name]
                result = scope       
                data_d = {}
                size = len(service_dependencies['node_order'])
                last_node = service_dependencies['node_order'][size-1]
                assert len(last_node) == 1
                last_node = last_node[0]

                request_sent_time = time.time()


                for node_list in service_dependencies['node_order']:
                    data_sent = {}
                    for node in node_list:
                        if len(service_dependencies['predecessors'][node]) == 0:
                            data_sent[node] = (scope,)
                        else:
                            predecessors_list = service_dependencies['predecessors'][node]
                            list_data = [data_d[p] for p in predecessors_list]
                            data_sent[node] = list_data

                    future_list = [ as_future(self.router.enqueue_request.remote(node, data_sent[node],slo=time.time())) for node in node_list]
                    completed_futures, non_c  = await asyncio.wait(future_list, return_when=asyncio.ALL_COMPLETED)
                    assert(len(non_c) == 0)
                    future_enqueues_binary = [f.result() for f in completed_futures]

                    future_enqueues = [ray.ObjectID(x) for x in future_enqueues_binary]
                    # completed_future_enqueues, non_c = ray.wait(future_enqueues,num_returns=len(future_enqueues))
                    # assert(len(non_c) == 0)
                    # node_data_list = ray.get(completed_future_enqueues)
                    for k,v in zip(node_list,future_enqueues):
                        data_d[k] = v
                        
                result = await as_future(data_d[last_node])

                result_received_time = time.time()

                self.profile_file.write(json.dumps({"start": request_sent_time, "end": result_received_time}))
                self.profile_file.write("\n")
                self.profile_file.flush()

                if isinstance(result, ray.exceptions.RayTaskError):
                    await JSONResponse({
                        "error": "internal error, please use python API to debug"
                    })(scope, receive, send)
                else:
                    await JSONResponse({"result": result})(scope, receive, send)

            elif scope['method'] == 'POST':
                body = await self.read_body(receive)
                assert type(body) is dict
                # result = body
                service_dependencies = self.pipeline_table[pipeline_name]

                result = body
                
                data_d = {}

                size = len(service_dependencies['node_order'])
                last_node = service_dependencies['node_order'][size-1]
                assert len(last_node) == 1
                last_node = last_node[0]

                request_sent_time = time.time()

                for node_list in service_dependencies['node_order']:
                    data_sent = {}
                    for node in node_list:
                        if len(service_dependencies['predecessors'][node]) == 0:
                            if 'data' in body:
                                data_sent[node] = (body['data'],)
                            else:
                                result = ray.exceptions.RayTaskError('Specify data in input', '')
                                break
                        else:
                            predecessors_list = service_dependencies['predecessors'][node]
                            list_data = [data_d[p] for p in predecessors_list]
                            data_sent[node] = list_data

                    future_list = [ as_future(self.router.enqueue_request.remote(node, data_sent[node],slo=time.time())) for node in node_list]
                    completed_futures, non_c  = await asyncio.wait(future_list, return_when=asyncio.ALL_COMPLETED)
                    assert(len(non_c) == 0)
                    future_enqueues_binary = [f.result() for f in completed_futures]

                    future_enqueues = [ray.ObjectID(x) for x in future_enqueues_binary]
                    # completed_future_enqueues, non_c = ray.wait(future_enqueues,num_returns=len(future_enqueues))
                    # assert(len(non_c) == 0)
                    # node_data_list = ray.get(completed_future_enqueues)
                    for k,v in zip(node_list,future_enqueues):
                        data_d[k] = v

                result = await as_future(data_d[last_node])

                result_received_time = time.time()

                self.profile_file.write(json.dumps({"start": request_sent_time, "end": result_received_time}))
                self.profile_file.write("\n")
                self.profile_file.flush()

                if isinstance(result, ray.exceptions.RayTaskError):
                    await JSONResponse({
                        "error": error_service + " internal error, please use python API to debug"
                    })(scope, receive, send)
                else:
                    await JSONResponse({"result": result})(scope, receive, send)
               
        else:
            error_message = ("Path {} not found. "
                             "Please ping http://.../ for routing table"
                             ).format(current_path)

            await JSONResponse(
                {
                    "error": error_message
                }, status_code=404)(scope, receive, send)


@ray.remote
class HTTPActor:
    def __init__(self, kv_store_actor_handle,kv_store_actor_pipeline_handle ,router_handle):
        self.app = HTTPProxy(kv_store_actor_handle,kv_store_actor_pipeline_handle,router_handle)

    def run(self, host="0.0.0.0", port=8000):
        uvicorn.run(self.app, host=host, port=port, lifespan="on")
