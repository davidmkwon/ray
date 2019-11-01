import ray
from ray.experimental import serve
import time

class RayServeHandle:
    """A handle to a service endpoint.

    Invoking this endpoint with .remote is equivalent to pinging
    an HTTP endpoint.

    Example:
       >>> handle = serve.get_handle("my_endpoint")
       >>> handle
       RayServeHandle(
            Endpoint="my_endpoint",
            URL="...",
            Traffic=...
       )
       >>> handle.remote(my_request_content)
       ObjectID(...)
       >>> ray.get(handle.remote(...))
       # result
       >>> ray.get(handle.remote(let_it_crash_request))
       # raises RayTaskError Exception
    """

    def __init__(self, kv_store_actor_handle_pipeline,router_handle, pipeline_name):
        self.router_handle = router_handle
        self.pipeline_name = pipeline_name
        self.kv_store_actor_handle_pipeline = kv_store_actor_handle_pipeline
        future = kv_store_actor_handle_pipeline.get_dependency.remote(pipeline_name)
        self.service_dependencies = ray.get(future)
    def remote(self, **args):
        # TODO(simon): Support kwargs once #5606 is merged.
        data_d = {}
        if 'slo' in args:
          args['slo'] = time.time() + float(args['slo'])/1000
        else:
          args['slo'] = time.time() + float(1e5)
        size = len(self.service_dependencies['node_order'])
        last_node = self.service_dependencies['node_order'][size-1]
        assert len(last_node) == 1
        last_node = last_node[0]
        for node_list in self.service_dependencies['node_order']:
          data_sent = {}
          for node in node_list:
              if len(self.service_dependencies['predecessors'][node]) == 0:
                  if node in args:
                      data_sent[node] = (args[node],)
                  else:
                      raise Exception('Specify service name in input')
              else:
                  predecessors_list = self.service_dependencies['predecessors'][node]
                  list_data = [data_d[p] for p in predecessors_list]
                  data_sent[node] = list_data

          future_list = [self.router_handle.enqueue_request.remote(node, data_sent[node],slo=args['slo']) for node in node_list]
          completed_futures, non_c  = ray.wait(future_list,num_returns=len(future_list))
          assert(len(non_c) == 0)
          future_enqueues_binary = ray.get(completed_futures)

          future_enqueues = [ray.ObjectID(x) for x in future_enqueues_binary]
          # completed_future_enqueues, non_c = ray.wait(future_enqueues,num_returns=len(future_enqueues))
          # assert(len(non_c) == 0)
          # node_data_list = ray.get(completed_future_enqueues)
          for k,v in zip(node_list,future_enqueues):
              data_d[k] = v
        # result_object_id_bytes = ray.get(
        #     self.router_handle.enqueue_request.remote(self.endpoint_name,
        #                                               *args))
        return data_d[last_node]

    # def get_traffic_policy(self):
    #     # TODO(simon): This method is implemented via checking global state
    #     # because we are sure handle and global_state are in the same process.
    #     # However, once global_state is deprecated, this method need to be
    #     # updated accordingly.
    #     history = serve.global_state.policy_action_history[self.endpoint_name]
    #     if len(history):
    #         return history[-1]
    #     else:
    #         return None

    def get_http_endpoint(self):
        return serve.global_state.http_address

#     def __repr__(self):
#         return """
# RayServeHandle(
#     Endpoint="{endpoint_name}",
#     URL="{http_endpoint}/{endpoint_name},
#     Traffic={traffic_policy}
# )
# """.format(endpoint_name=self.endpoint_name,
#            http_endpoint=self.get_http_endpoint(),
#            traffic_policy=self.get_traffic_policy())

    # TODO(simon): a convenience function that dumps equivalent requests
    # code for a given call.
