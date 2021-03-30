import click
import time
from blist import sortedlist
from collections import defaultdict, deque

import ray
from ray import ObjectRef

class Query:
    def __init__(self, request_body,slo, result_object_id=None):
        self.request_body = request_body
        self.slo = slo
        if result_object_id is None:
            self.result_object_id = ObjectRef.owned_plasma_objectref()
        else:
            self.result_object_id = result_object_id

    def __lt__(self, other):
        return self.slo > other.slo


class WorkIntent:
    def __init__(self, work_object_id=None):
        if work_object_id is None:
            self.work_object_id = ObjectRef.owned_plasma_objectref()
        else:
            self.work_object_id = work_object_id


class CentralizedQueues:
    """A router that routes request to available workers.

    Router aceepts each request from the `enqueue_request` method and enqueues
    it. It also accepts worker request to work (called work_intention in code)
    from workers via the `dequeue_request` method. The traffic policy is used
    to match requests with their corresponding workers.

    Behavior:
        >>> # psuedo-code
        >>> queue = CentralizedQueues()
        >>> queue.enqueue_request('service-name', data)
        # nothing happens, request is queued.
        # returns result ObjectID, which will contains the final result
        >>> queue.dequeue_request('backend-1')
        # nothing happens, work intention is queued.
        # return work ObjectID, which will contains the future request payload
        >>> queue.link('service-name', 'backend-1')
        # here the enqueue_requester is matched with worker, request
        # data is put into work ObjectID, and the worker processes the request
        # and store the result into result ObjectID

    Traffic policy splits the traffic among different workers
    probabilistically:

    1. When all backends are ready to receive traffic, we will randomly
       choose a backend based on the weights assigned by the traffic policy
       dictionary.

    2. When more than 1 but not all backends are ready, we will normalize the
       weights of the ready backends to 1 and choose a backend via sampling.

    3. When there is only 1 backend ready, we will only use that backend.
    """

    def __init__(self):
        # service_name -> request queue
        self.queues = defaultdict(sortedlist)
        # service_name -> max. batch size
        self.service_max_batch_size = {}
        # service_name -> traffic_policy
        self.traffic = defaultdict(dict)
        # backend_name -> worker queue
        self.worker_handles = defaultdict(deque)

    def enqueue_request(self, service, request_data, slo=float(1e10)):
        query = Query(request_data, slo)
        self.queues[service].add(query)
        self.flush()
        return query.result_object_id

    def set_max_batch(self, service, max_batch):
        self.service_max_batch_size[service] = max_batch

    def dequeue_request(self, backend, worker_handle):
        #intention = WorkIntent()
        #TODO: wrap all OIDs passed in lists
        self.worker_handles[backend].append(worker_handle)
        self.flush()
        #return intention.work_object_id

    def link(self, service, backend):
        #logger.debug("Link %s with %s", service, backend)
        self.traffic[service][backend] = 1.0
        self.flush()

    def set_traffic(self, service, traffic_dict):
        #logger.debug("Setting traffic for service %s to %s", service, traffic_dict)
        self.traffic[service] = traffic_dict
        self.flush()

    def flush(self):
        """In the default case, flush calls ._flush.

        When this class is a Ray actor, .flush can be scheduled as a remote
        method invocation.
        """
        self._flush_single()

    def _get_available_backends(self, service):
        backends_in_policy = set(self.traffic[service].keys())
        available_workers = {
            backend
            for backend, queues in self.worker_handles.items() if len(queues) > 0
        }
        return list(backends_in_policy.intersection(available_workers))

    def _flush_batch(self):
        for service, queue in self.queues.items():
            ready_backends = self._get_available_backends(service)
            # logger.info("Service %s having queue lengths %s ready backends %s", service,len(queue),len(ready_backends))
            while len(queue) and len(ready_backends):
                #batch_size = self.service_max_batch_size[service]
                batch_size = 5
                for backend in ready_backends:
                    if len(queue) == 0:
                        break
                    worker_handle = self.worker_handles[backend].popleft()
                    pop_len = min(batch_size,len(queue))
                    request = [ queue.pop() for i in range(pop_len)]
                    #print("putting request into work OID")
                    worker_handle.__call__.remote(request)
                    #print("called worker execute")

                ready_backends = self._get_available_backends(service)

    def _flush_single(self):
        for service, queue in self.queues.items():
            ready_backends = self._get_available_backends(service)
            # logger.info("Service %s having queue lengths %s ready backends %s", service,len(queue),len(ready_backends))
            while len(queue) and len(ready_backends):
                for backend in ready_backends:
                    if len(queue) == 0:
                        break
                    worker_handle = self.worker_handles[backend].popleft()
                    request = [queue.pop()]
                    #print("put request in work OID")
                    worker_handle.__call__.remote(request)
                    #print("called worker execute")

                ready_backends = self._get_available_backends(service)


@ray.remote
class CentralizedQueuesActor(CentralizedQueues):
    self_handle = None

    def register_self_handle(self, handle_to_this_actor):
        self.self_handle = handle_to_this_actor

    def flush(self):
        if self.self_handle:
            self.self_handle._flush_single.remote()
        else:
            self._flush_single()
