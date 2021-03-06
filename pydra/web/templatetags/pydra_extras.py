"""
    Copyright 2009 Oregon State University

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

import datetime

from django import template
from django.utils.safestring import mark_safe
register = template.Library()

from pydra.cluster.tasks import STATUS_CANCELLED, STATUS_FAILED, STATUS_STOPPED, STATUS_RUNNING, STATUS_PAUSED, STATUS_COMPLETE


@register.filter(name='available_cores')
def available_cores(node):
    """
    Filter that returns the the number of available cores on a node
    """
    if node['cores_available']:
        return node['cores_available']
    else:
        return node['cores']
register.filter('available_cores',available_cores)


@register.filter(name='node_range')
def node_range(node):
    """
    Filter that creates a range equal to the number of cores available on a node
    """
    if node['cores_available']:
        return range(0,node['cores_available'])

    #default to all cores
    if node['cores']:
        return range(0,node['cores'])

    #node hasn't been initialized, we don't know how many cores it has
    return None

register.filter('node_range',node_range)


@register.filter(name='task_description')
def task_description(tasks, key):
    """
    Filter that retrieves a tasks description from a dict of tasks
    using task_key to look it up
    """
    return tasks[key].description
register.filter('task_description',task_description)


@register.filter(name='task_status')
def task_status(code):
    """
    Filter that replaces a task code with an icon
    """
    if code == STATUS_RUNNING:
        css_class = "task_status_running"
        title = "running"

    elif code == STATUS_COMPLETE:
        css_class = "task_status_complete"
        title = "completed succesfully"

    elif code == STATUS_STOPPED:
        css_class = "task_status_stopped"
        title = "queued"

    elif code == STATUS_CANCELLED:
        css_class = "task_status_cancelled"
        title = "cancelled by user"

    elif code == STATUS_FAILED:
        css_class = "task_status_failed"
        title = "failed"

    elif code == STATUS_PAUSED:
        css_class = "task_status_paused"
        title = "paused"

    else:
        css_class = "task_status_unknown"
        title = "unknown status"

    return mark_safe('<div class="icon %s" title="%s"></div>' % (css_class, title))
register.filter('task_status', task_status)



@register.filter(name='task_status_text')
def task_status_text(code):
    """
    Filter that replaces a task code with text
    """
    if code == STATUS_RUNNING:
        return "running"

    elif code == STATUS_COMPLETE:
        return "completed succesfully"

    elif code == STATUS_STOPPED:
        return "queued"

    elif code == STATUS_CANCELLED:
       return "cancelled by user"

    elif code == STATUS_FAILED:
        return "failed"

    elif code == STATUS_PAUSED:
        return "paused"

    else:
        return "unknown status"
register.filter('task_status_text', task_status_text)


@register.filter(name='no_escape')
def no_escape(string):
    """
    Filter that renders string with no escaping
    """
    return mark_safe(string)
register.filter('no_escape', no_escape)


@register.filter(name='int_date')
def int_date(int_):
    """
    Filter that converts an int to a DateTime
    """
    return datetime.datetime.fromtimestamp(int_)
register.filter('int_date', int_date)


@register.filter(name="today")
def today(then):
    """
    Formatting filter for datetimes that special-cases today.
    """

    today = datetime.date.today()
    if today == then.date():
        formatted = then.strftime("%H:%M:%S")
    else:
        formatted = then.strftime("%Y-%m-%d %H:%M:%S")

    return formatted


@register.filter(name='generic')
def generic(obj):
    """
    Filter that aides in rendering the structure of an unknown object.  This
    function returns a template name that either recurses into a list/dict or
    displays a value.
    """
    if isinstance(obj, (list, tuple)):
        return 'generic/list.html'
    elif isinstance(obj, (dict, )):
        return 'generic/dict.html'
    return 'generic/value.html'


@register.filter(name='more')
def more(content, length=50):
    """
    Limits length of content.  If the content goes beyond the specified amount
    the maximum amount is displayed with a "more" link.  The full content
    is placed in a <div class="more"> tag.

    The div tag can then be used for a popup/drilldown/expander/etc that
    displays the content when "more" is clicked on.  This filter does not
    define what that is.
    """
    if len(content) < length:
        return content

    word_break = content[:length-8].rfind(' ')
    return mark_safe('%s  <span class="more_button">... more</span><div class="more">%s</div>' \
                     % (content[:word_break], content))
