from django.core.paginator import Paginator, InvalidPage, EmptyPage

from pydra.cluster.module import Module
from pydra.models import Node

import logging
logger = logging.getLogger('root')

class NodeManager(Module):
    """
    Module for managing nodes.  Provides functionality for listing, creating, and editing nodes
    """

    _signals = [
        'NODE_CREATED'
        'NODE_DELETED',
        'NODE_EDITED'
    ]

    _shared = [
        'workers',
        '_idle_workers',
        '_active_workers',
        'nodes'
    ]

    def __init__(self):
        self._interfaces = [
            self.kill_worker,
            self.node_list,
            self.node_detail,
            self.node_edit,
            self.node_delete,
            self.node_status
        ]


    def node_detail(self, id):
        """
        Returns details for a single node
        """
        node = Node.objects.get(id=id)
        return node.json_safe()
        

    def node_edit(self, values):
        """
        Updates or Creates a node with the values passed in.  If an id field
        is present it will be update the existing node.  Otherwise it will
        create a new node
        """
        if values.has_key('id'):
            node = Node.objects.get(pk=values['id'])
            new = False
        else:
            node = Node()
            new = True

        for k,v in values.items():
            node.__dict__[k] = v
        node.save()

        #emit signals
        if new:
            self.emit('NODE_CREATED', node)

        else:            
            self.emit('NODE_UPDATED', node)


    def node_delete(self, id):
        """
        deletes a node with the id passed in.
        """
        node = Node.objects.get(id=id)
        node.deleted = True
        node.save()
        self.emit('NODE_DELETED', node)


    def node_list(self, page=1):
        """
        Lists Nodes saved in the database
        """
        # get nodes
        nodes = Node.objects.exclude(deleted=True)

        # paginate
        paginator = Paginator(nodes, 25) # Show 25 nodes per page

        # Make sure page request is an int. If not, deliver first page.
        try:
            page = int(page)
        except ValueError:
            page = 1

        # If page request (9999) is out of range, deliver last page of results.
        try:
            paginatedNodes = paginator.page(page)
        except (EmptyPage, InvalidPage):
            page = paginator.num_pages
            paginatedNodes = paginator.page(page)
        instances = [node.json_safe() for node in paginatedNodes.object_list]

        #generate a list of pages to display in the pagination bar
        pages = ([i for i in range(1, 11 if page < 8 else 3)],
                [i for i in range(page-5,page+5)] if page > 7 and page < paginator.num_pages-6 else None,
                [i for i in range(paginator.num_pages-(1 if page < paginator.num_pages-6 else 9), paginator.num_pages+1)])
    
        return instances, pages


    def node_status(self):
        """
        Returns status information about Nodes and Workers in the cluster
        """
        node_status = {}
        #iterate through all the nodes adding their status
        for key, node in self.nodes.items():
            worker_status = {}
            if node.cores:
                #iterate through all the workers adding their status as well
                #also check for a worker whose should be running but is not connected
                for i in range(node.cores):
                    w_key = '%s:%s:%i' % (node.host, node.port, i)
                    html_key = '%s_%i' % (node.id, i)
                    if w_key in self._idle_workers:
                        worker_status[html_key] = (1,-1,-1)
                    elif w_key in self._active_workers:
                        job = self._active_workers[w_key]
                        worker_status[html_key] = (1, job.task_key, \
                                job.subtask_key, \
                                job.workunit if job.workunit else -1)
                    else:
                        worker_status[html_key] = -1

            else:
                worker_status=-1

            node_status[key] = {'status':node.status(),
                                'workers':worker_status
                            }

        return node_status


    def kill_worker(self, worker_key, kill=False, fail=True):
        """
        Force a worker process to terminate.
        
        @param worker_key - id of worker to kill
        @param kill [False] - send a SIGKILL to process instead of SIGTERM
        @param fail [True] - workunit should return as if it failed.
        """
        worker = self.workers[worker_key].remote
        worker.callRemote('kill_worker', kill, fail)
