#
# Copyright (c) 2010 Greek Research and Technology Network
#

from synnefo.api.errors import *
from synnefo.api.util import *
from synnefo.db.models import *
from synnefo.util.rapi import GanetiRapiClient

from django.conf.urls.defaults import *
from django.http import HttpResponse
from django.template.loader import render_to_string

from logging import getLogger

import json


log = getLogger('synnefo.api.servers')
rapi = GanetiRapiClient(*settings.GANETI_CLUSTER_INFO)

urlpatterns = patterns('synnefo.api.servers',
    (r'^(?:/|.json|.xml)?$', 'demux'),
    (r'^/detail(?:.json|.xml)?$', 'list_servers', {'detail': True}),
    (r'^/(\d+)(?:.json|.xml)?$', 'server_demux'),
)


def demux(request):
    if request.method == 'GET':
        return list_servers(request)
    elif request.method == 'POST':
        return create_server(request)
    else:
        fault = BadRequest()
        return render_fault(request, fault)

def server_demux(request, server_id):
    if request.method == 'GET':
        return get_server_details(request, server_id)
    elif request.method == 'PUT':
        return update_server_name(request, server_id)
    elif request.method == 'DELETE':
        return delete_server(request, server_id)
    else:
        fault = BadRequest()
        return render_fault(request, fault)


def server_to_dict(server, detail=False):
    d = dict(id=server.id, name=server.name)
    if detail:
        d['status'] = server.rsapi_state
        d['progress'] = 100 if server.rsapi_state == 'ACTIVE' else 0
        d['hostId'] = server.hostid
        d['updated'] = server.updated.isoformat()
        d['created'] = server.created.isoformat()
        d['flavorId'] = server.flavor.id            # XXX Should use flavorRef instead?
        d['imageId'] = server.sourceimage.id        # XXX Should use imageRef instead?
        d['description'] = server.description       # XXX Not in OpenStack docs
        
        server_meta = server.virtualmachinemetadata_set.all()
        metadata = dict((meta.meta_key, meta.meta_value) for meta in server_meta)
        if metadata:
            d['metadata'] = dict(values=metadata)
        
        public_addrs = [dict(version=4, addr=server.ipfour), dict(version=6, addr=server.ipsix)]
        d['addresses'] = {'values': []}
        d['addresses']['values'].append({'id': 'public', 'values': public_addrs})
    return d

def render_server(request, serverdict, status=200):
    if request.type == 'xml':
        data = render_to_string('server.xml', dict(server=serverdict, is_root=True))
    else:
        data = json.dumps({'server': serverdict})
    return HttpResponse(data, status=status)


@api_method('GET')
def list_servers(request, detail=False):
    # Normal Response Codes: 200, 203
    # Error Response Codes: computeFault (400, 500),
    #                       serviceUnavailable (503),
    #                       unauthorized (401),
    #                       badRequest (400),
    #                       overLimit (413)
    
    owner = get_user()
    user_servers = VirtualMachine.objects.filter(owner=owner, deleted=False)
    servers = [server_to_dict(server, detail) for server in user_servers]
    
    if request.type == 'xml':
        data = render_to_string('list_servers.xml', dict(servers=servers, detail=detail))
    else:
        data = json.dumps({'servers': {'values': servers}})
    
    return HttpResponse(data, status=200)

@api_method('POST')
def create_server(request):
    # Normal Response Code: 202
    # Error Response Codes: computeFault (400, 500),
    #                       serviceUnavailable (503),
    #                       unauthorized (401),
    #                       badMediaType(415),
    #                       itemNotFound (404),
    #                       badRequest (400),
    #                       serverCapacityUnavailable (503),
    #                       overLimit (413)
    
    req = get_request_dict(request)
    
    try:
        server = req['server']
        name = server['name']
        sourceimage = Image.objects.get(id=server['imageId'])
        flavor = Flavor.objects.get(id=server['flavorId'])
    except KeyError:
        raise BadRequest
    except Image.DoesNotExist:
        raise ItemNotFound
    except Flavor.DoesNotExist:
        raise ItemNotFound
    
    server = VirtualMachine.objects.create(
        name=name,
        owner=get_user(),
        sourceimage=sourceimage,
        ipfour='0.0.0.0',
        ipsix='::1',
        flavor=flavor)
                
    if request.META.get('SERVER_NAME', None) == 'testserver':
        name = 'test-server'
        dry_run = True
    else:
        name = server.backend_id
        dry_run = False
    
    jobId = rapi.CreateInstance(
        mode='create',
        name=name,
        disk_template='plain',
        disks=[{"size": 2000}],         #FIXME: Always ask for a 2GB disk for now
        nics=[{}],
        os='debootstrap+default',       #TODO: select OS from imageId
        ip_check=False,
        nam_check=False,
        pnode=rapi.GetNodes()[0],       #TODO: verify if this is necessary
        dry_run=dry_run,
        beparams=dict(auto_balance=True, vcpus=flavor.cpu, memory=flavor.ram))
    
    server.save()
        
    log.info('created vm with %s cpus, %s ram and %s storage' % (flavor.cpu, flavor.ram, flavor.disk))
    
    serverdict = server_to_dict(server, detail=True)
    serverdict['status'] = 'BUILD'
    serverdict['adminPass'] = random_password()
    return render_server(request, serverdict, status=202)

@api_method('GET')
def get_server_details(request, server_id):
    # Normal Response Codes: 200, 203
    # Error Response Codes: computeFault (400, 500),
    #                       serviceUnavailable (503),
    #                       unauthorized (401),
    #                       badRequest (400),
    #                       itemNotFound (404),
    #                       overLimit (413)
    
    try:
        server_id = int(server_id)
        server = VirtualMachine.objects.get(id=server_id)
    except VirtualMachine.DoesNotExist:
        raise ItemNotFound
    
    serverdict = server_to_dict(server, detail=True)
    return render_server(request, serverdict)

@api_method('PUT')
def update_server_name(request, server_id):
    # Normal Response Code: 204
    # Error Response Codes: computeFault (400, 500),
    #                       serviceUnavailable (503),
    #                       unauthorized (401),
    #                       badRequest (400),
    #                       badMediaType(415),
    #                       itemNotFound (404),
    #                       buildInProgress (409),
    #                       overLimit (413)
    
    req = get_request_dict(request)
    
    try:
        name = req['server']['name']
        server_id = int(server_id)
        server = VirtualMachine.objects.get(id=server_id)
    except KeyError:
        raise BadRequest
    except VirtualMachine.DoesNotExist:
        raise ItemNotFound
    
    server.name = name
    server.save()
    
    return HttpResponse(status=204)

@api_method('DELETE')
def delete_server(request, server_id):
    # Normal Response Codes: 204
    # Error Response Codes: computeFault (400, 500),
    #                       serviceUnavailable (503),
    #                       unauthorized (401),
    #                       itemNotFound (404),
    #                       unauthorized (401),
    #                       buildInProgress (409),
    #                       overLimit (413)
    
    try:
        server_id = int(server_id)
        server = VirtualMachine.objects.get(id=server_id)
    except VirtualMachine.DoesNotExist:
        raise ItemNotFound
    
    server.start_action('DESTROY')
    rapi.DeleteInstance(server.backend_id)
    return HttpResponse(status=204)
