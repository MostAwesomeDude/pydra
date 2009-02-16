"""
    This file is part of Pydra.

    Pydra is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Pydra is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Pydra.  If not, see <http://www.gnu.org/licenses/>.
"""

from django.shortcuts import render_to_response
from django.template import RequestContext
from django.http import HttpResponseRedirect
from django.core.paginator import Paginator, InvalidPage, EmptyPage
from django.utils import simplejson
from django.http import HttpResponse

import math

from pydra_server.models import Node, TaskInstance, pydraSettings
from cluster.amf_controller import AMFController
from forms import NodeForm
from models import pydraSettings
import settings


"""
pydraController is a global variable that stores an instance of a Controller.
The current controller does not involve much setup so this may not be required
any longer, but it will improve resource usage slightly
"""
pydra_controller = AMFController(pydraSettings.host , pydraSettings.port)

"""
Pydra_processor is used by any view that will need information or control
over the master server
"""
def pydra_processor(request):
    global pydra_controller

    if pydra_controller == None:
        pydra_controller = AMFController(pydraSettings.host , pydraSettings.port)

    return {'controller':pydra_controller}



"""
display nodes
"""
def nodes(request):
    # get nodes
    nodes = Node.objects.all()

    # paginate
    paginator = Paginator(nodes, 25) # Show 25 segments per page

    # Make sure page request is an int. If not, deliver first page.
    try:
        page = int(request.GET.get('page', '1'))
    except ValueError:
        page = 1

    # If page request (9999) is out of range, deliver last page of results.
    try:
        paginatedNodes = paginator.page(page)
    except (EmptyPage, InvalidPage):
        page = paginator.num_pages
        paginatedNodes = paginator.page(page)

    #generate a list of pages to display in the pagination bar
    pages = ([i for i in range(1, 11 if page < 8 else 3)],
            [i for i in range(page-5,page+5)] if page > 7 and page < paginator.num_pages-6 else None,
            [i for i in range(paginator.num_pages-(1 if page < paginator.num_pages-6 else 9), paginator.num_pages+1)])

    return render_to_response('nodes.html', {
        'nodes':paginatedNodes,
        'pages':pages,
    }, context_instance=RequestContext(request, processors=[pydra_processor]))


"""
Handler for creating and editing nodes
"""
def node_edit(request, id=None):
    if request.method == 'POST': 
        if id:
            node = Node(pk=id)
            form = NodeForm(request.POST, instance=node) 
        else:
            form = NodeForm(request.POST)

        if form.is_valid():
            form.save()
            return HttpResponseRedirect('%s/nodes' % settings.SITE_ROOT) # Redirect after POST

    else:
        if id:
            node = Node.objects.get(pk=id)
            form = NodeForm(instance=node)
        else:
            # An unbound form
            form = NodeForm() 

    return render_to_response('node_edit.html', {
        'form': form,
        'id':id,
    }, context_instance=RequestContext(request))


"""
Retrieves Status of nodes
"""
def node_status(request):
    c = RequestContext(request, {
        'MEDIA_URL': settings.MEDIA_URL
    }, [pydra_processor])
    return HttpResponse(pydra_controller.remote_node_status(), mimetype='application/javascript')


"""
handler for displaying jobs
"""
def jobs(request):
    tasks = pydra_controller.remote_list_tasks()
    queue = TaskInstance.objects.queued()
    running = TaskInstance.objects.running()

    return render_to_response('tasks.html', {
        'MEDIA_URL': settings.MEDIA_URL,
        'tasks': tasks,
        'queue': queue,
        'running': running,
    }, context_instance=RequestContext(request, processors=[pydra_processor]))

"""
Handler for retrieving status 
"""
def task_progress(request):
    c = RequestContext(request, {
        'MEDIA_URL': settings.MEDIA_URL
    }, [pydra_processor])

    return HttpResponse(pydra_controller.remote_task_status(), mimetype='application/javascript')


def run_task(request):
    key = request.POST['key']

    c = RequestContext(request, {
        'MEDIA_URL': settings.MEDIA_URL
    }, [pydra_processor])

    return HttpResponse(pydra_controller.remote_run_task(key), mimetype='application/javascript')

