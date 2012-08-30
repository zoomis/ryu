# Copyright (C) 2012 Nippon Telegraph and Telephone Corporation.
# Copyright (C) 2012 Isaku Yamahata <yamahata at private email ne jp>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from webob import Request, Response

from ryu.base import app_manager
from ryu.controller import network
from ryu.controller.flowvisor_cli import FlowVisor_CLI
from ryu.controller import dpset
from ryu.exception import NetworkNotFound, NetworkAlreadyExist
from ryu.exception import PortNotFound, PortAlreadyExist, PortUnknown
from ryu.app.wsgi import ControllerBase, WSGIApplication
from ryu.exception import MacAddressDuplicated

## TODO:XXX
## define db interface and store those information into db

# REST API

# get the list of networks
# GET /v1.0/networks/
#
# register a new network.
# Fail if the network is already registered.
# POST /v1.0/networks/{network-id}
#
# update a new network.
# Success as nop even if the network is already registered.
#
# PUT /v1.0/networks/{network-id}
#
# remove a network
# DELETE /v1.0/networks/{network-id}
#
# get the list of sets of dpid and port
# GET /v1.0/networks/{network-id}/
#
# register a new set of dpid and port
# Fail if the port is already registered.
# POST /v1.0/networks/{network-id}/{dpid}_{port-id}
#
# update a new set of dpid and port
# Success as nop even if same port already registered
# PUT /v1.0/networks/{network-id}/{dpid}_{port-id}
#
# remove a set of dpid and port
# DELETE /v1.0/networks/{network-id}/{dpid}_{port-id}

# We store networks and ports like the following:
#
# {network_id: [(dpid, port), ...
# {3: [(3,4), (4,7)], 5: [(3,6)], 1: [(5,6), (4,5), (4, 10)]}
#

class NetworkController(ControllerBase):
    def __init__(self, req, link, data, **config):
        super(NetworkController, self).__init__(req, link, data, **config)
        self.nw = data

    def create(self, req, network_id, **_kwargs):
        try:
            self.nw.create_network(network_id)
        except NetworkAlreadyExist:
            return Response(status=409)
        else:
            return Response(status=200)

    def update(self, req, network_id, **_kwargs):
        self.nw.update_network(network_id)
        return Response(status=200)

    def lists(self, req, **_kwargs):
        body = json.dumps(self.nw.list_networks())
        return Response(content_type='application/json', body=body)

    def list_macs(self, req, network_id, **_kwargs):
        mac_list = []
        for macAddr in self.nw.list_macs(network_id):
            macStr = ""
            for byte in map(ord, macAddr): #Converts byte-wise mac addr to proper string
                macStr += "%X" % byte + "-"
            
            mac_list.append(macStr[:-1])
            
        body = json.dumps(mac_list)
        return Response(content_type='application/json', body=body)

    def add_mac(self, req, network_id, mac, **_kwargs):
        try:
            self.nw.add_mac(network_id, mac)
        except MacAddressDuplicated:
            return Response(status=409)
        else:
            return Response(status=200)

    def add_iface(self, req, network_id, iface_id, **_kwargs):
        try:
            self.nw.add_iface(network_id, iface_id)
        except MacAddressDuplicated:
            return Response(status=409)
        else:
            return Response(status=200)

    def del_mac(self, req, network_id, mac, **_kwargs):
        try:
            # 'network_id' not actually required
            # Kept to keep uri format similar to add_mac
            self.nw.del_mac(mac)
        except:
            return Response(status=500)
        else:
            return Response(status=200)

    def del_iface(self, req, network_id, iface_id, **_kwargs):
        try:
            # 'network_id' not actually required
            # Kept to keep uri format similar to add_iface
            self.nw.del_iface(iface_id)
        except:
            return Response(status=500)
        else:
            return Response(status=200)

    def delete(self, req, network_id, **_kwargs):
        try:
            self.nw.remove_network(network_id)
        except NetworkNotFound:
            return Response(status=404)

        return Response(status=200)

    def setPacketHandler(self, req, handler_id, **_kwargs):
        self.nw.setPacketHandler(int(handler_id))
        return Response(status=200)


class PortController(ControllerBase):
    def __init__(self, req, link, data, **config):
        super(PortController, self).__init__(req, link, data, **config)
        self.nw = data.get('nw')
        self.fv_cli = data.get('fv_cli')
        assert self.fv_cli is not None
        assert self.nw is not None

    def create(self, req, network_id, dpid, port_id, **_kwargs):
        try:
            self.fv_cli.updatePort(network_id, int(dpid, 16),
                                    int(port_id), False)
            self.nw.create_port(network_id, int(dpid, 16), int(port_id))
        except NetworkNotFound:
            return Response(status=404)
        except PortAlreadyExist:
            return Response(status=409)

        return Response(status=200)

    def update(self, req, network_id, dpid, port_id, **_kwargs):
        try:
            try:
                old_network_id = self.nw.get_network(int(dpid, 16), int(port_id))
            except PortUnknown:
                old_network_id = None

            self.fv_cli.updatePort(network_id, int(dpid, 16),
                                    int(port_id), True, old_network_id)
            self.nw.update_port(network_id, int(dpid, 16), int(port_id))
        except (NetworkNotFound, PortUnknown):
            return Response(status=404)

        return Response(status=200)

    def lists(self, req, network_id, **_kwargs):
        try:
            body = json.dumps(self.nw.list_ports(network_id))
        except NetworkNotFound:
            return Response(status=404)

        return Response(content_type='application/json', body=body)

    def delete(self, req, network_id, dpid, port_id, **_kwargs):
        try:
            self.fv_cli.deletePort(network_id, int(dpid, 16), int(port_id))
            self.nw.remove_port(network_id, int(dpid, 16), int(port_id))
        except (NetworkNotFound, PortNotFound):
            return Response(status=404)

        return Response(status=200)


class FlowVisorController(ControllerBase):
    def __init__(self, req, link, data, **config):
        super(FlowVisorController, self).__init__(req, link, data, **config)
        self.fv_cli = data.get('fv_cli')
        self.nw = data.get('nw')
        self.dpset = data.get('dpset')
        assert self.fv_cli is not None
        assert self.nw is not None
        assert self.dpset is not None

    def listSlices(self, req, **_kwargs):
        body = json.dumps(self.fv_cli.listSlices())
        if (body.find("Connection refused") > 0):
            status = 500
        else:
            status = 200

        return Response(status=status, content_type='application/json', body=body)

    def createSlice(self, req, sliceName, ip, port, pwd):
        body = json.dumps(self.fv_cli.createSlice(sliceName, ip, port, pwd))
        if (body.find("success") > 0):
            status = 200
        elif (body.find("Cannot create slice with existing name") > 0):
            status = 409
        else:
            status = 500

        return Response(status=status, content_type='application/json', body=body)

    def deleteSlice(self, req, sliceName):
        body = json.dumps(self.fv_cli.deleteSlice(sliceName))
        if (body.find("success") > 0):
            status = 200
        elif (body.find("slice does not exist") > 0):
            status = 409
        else:
            status = 500

        return Response(status=status, content_type='application/json', body=body)

    # Delegate control of a network to the controller in charge of the specified slice
    def assignNetwork(self, req, sliceName, network_id):
        status = 200
        ret = ""

        # Check if network has been assigned to another controller
        # If so, must unassign it from the other controller first
        slice = self.fv_cli.getSliceName(network_id)
        if slice:
            if (slice == sliceName):  # Should this result in an error instead?
                return Response(status=status)

            response = self.unassignNetwork(req, network_id)
            status = response.status_code
        
        if (status == 200):
            # Install FV rules to route packets to controller
            for (dpid, port) in self.nw.list_ports(network_id):
                ret = self.fv_cli.addFlowSpace(sliceName, dpid, port)
                if (ret.find("success") < 0):
                    # Error occured while attempting to install FV rule
                    status = 500
                    break

                # Keep track of installed rules related to network
                self.fv_cli.addFlowSpaceID(dpid, port, ret[9:])

                # Now delete rules installed in the switches
                dp = self.dpset.get(dpid)
                if dp is not None:
                    dp.send_delete_all_flows()

        if (status == 500):
            # Error occured in the middle of installing rules
            # Previously installed rules be deleted
            self.unassignNetwork(req, network_id)

        if (status == 200):
            self.fv_cli.slice2nw_add(sliceName, network_id)

        body = json.dumps(ret)
        return Response(status=status, content_type='application/json', body=body)

    def unassignNetwork(self, req, network_id):
        status = 200
        ret = ""

        # Check if network has been assigned to a controller
        if self.fv_cli.getSliceName(network_id):
            # Remove FlowSpace rules associated with the network
            ids = self.nw.list_ports(network_id)
            if ids is not None:
                for (dpid, port) in ids:
                    try:
                        flowspace_id = self.fv_cli.getFlowSpaceID(dpid, port)
                    except PortUnknown:
                        # Continue instead of break due to possibility
                        #    this function was called to remedy a half-
                        #    completed call to assignNetwork
                        status = 404
                        continue

                    ret = self.fv_cli.removeFlowSpace(flowspace_id)
                    if (ret.find("success") < 0):
                        status = 500
                        break

                    self.fv_cli.delFlowSpaceID(dpid, port)
            else:
                status = 404

            # Now delete rules installed in relevant switches
            for (dpid, port) in self.nw.list_ports(network_id):
                dp = self.dpset.get(dpid)
                if dp is not None:
                    dp.send_delete_all_flows()

            if (status == 200):
                self.fv_cli.slice2nw_del(network_id)
        else:
            # Should this result in an error status and message instead?
            ret = "success!"

        body = json.dumps(ret)
        return Response(status=status, content_type='application/json', body=body)


class restapi(app_manager.RyuApp):
    _CONTEXTS = {
        'network': network.Network,
        'wsgi': WSGIApplication,
        'fv_cli' : FlowVisor_CLI,
        'dpset': dpset.DPSet
        }

    def __init__(self, *args, **kwargs):
        super(restapi, self).__init__(*args, **kwargs)
        self.nw = kwargs['network']
        self.fv_cli = kwargs['fv_cli']
        wsgi = kwargs['wsgi']
        self.dpset = kwargs['dpset']
        mapper = wsgi.mapper

        # Change packet handler
        wsgi.registory['NetworkController'] = self.nw
        mapper.connect('networks', '/v1.0/packethandler/{handler_id}',
                       controller=NetworkController, action='setPacketHandler',
                       conditions=dict(method=['PUT']))
        
        uri = '/v1.0/networks'
        mapper.connect('networks', uri,
                       controller=NetworkController, action='lists',
                       conditions=dict(method=['GET', 'HEAD']))

        uri += '/{network_id}'
        mapper.connect('networks', uri,
                       controller=NetworkController, action='create',
                       conditions=dict(method=['POST']))

        mapper.connect('networks', uri,
                       controller=NetworkController, action='update',
                       conditions=dict(method=['PUT']))

        mapper.connect('networks', uri,
                       controller=NetworkController, action='delete',
                       conditions=dict(method=['DELETE']))

        # List macs associated with a network
        mapper.connect('networks', uri + '/macs',
                       controller=NetworkController, action='list_macs',
                       conditions=dict(method=['GET']))

        # Associate a MAC address with a network
        mapper.connect('networks', uri + '/macs/{mac}',
                       controller=NetworkController, action='add_mac',
                       conditions=dict(method=['PUT']))

        # Associate an interface with a network
        mapper.connect('networks', uri + '/iface/{iface_id}',
                       controller=NetworkController, action='add_iface',
                       conditions=dict(method=['PUT']))

        # Dissociate a MAC address with a network
        mapper.connect('networks', uri + '/macs/{mac}',
                       controller=NetworkController, action='del_mac',
                       conditions=dict(method=['DELETE']))

        # Dissociate an interface with a network
        mapper.connect('networks', uri + '/iface/{iface_id}',
                       controller=NetworkController, action='del_iface',
                       conditions=dict(method=['DELETE']))

        wsgi.registory['PortController'] = {"nw" : self.nw,
                                            "fv_cli" : self.fv_cli}
        mapper.connect('networks', uri,
                       controller=PortController, action='lists',
                       conditions=dict(method=['GET']))

        uri += '/{dpid}_{port_id}'
        mapper.connect('ports', uri,
                       controller=PortController, action='create',
                       conditions=dict(method=['POST']))
        mapper.connect('ports', uri,
                       controller=PortController, action='update',
                       conditions=dict(method=['PUT']))

        mapper.connect('ports', uri,
                       controller=PortController, action='delete',
                       conditions=dict(method=['DELETE']))

        # FlowVisor related APIs
        wsgi.registory['FlowVisorController'] = {'fv_cli' : self.fv_cli,
                                                 'nw' : self.nw,
                                                 'dpset' : self.dpset}
        uri = '/v1.0/flowvisor'
        mapper.connect('flowvisor', uri,
                       controller=FlowVisorController, action='listSlices',
                       conditions=dict(method=['GET']))

        mapper.connect('flowvisor', uri + '/{sliceName}',
                       controller=FlowVisorController, action='deleteSlice',
                       conditions=dict(method=['DELETE']))

        mapper.connect('flowvisor', uri + '/{sliceName}_{ip}_{port}_{pwd}',
                       controller=FlowVisorController, action='createSlice',
                       conditions=dict(method=['POST']))

        mapper.connect('flowvisor', uri + '/{sliceName}/assign/{network_id}',
                       controller=FlowVisorController, action='assignNetwork',
                       conditions=dict(method=['PUT']))

        mapper.connect('flowvisor', uri + '/unassign/{network_id}',
                       controller=FlowVisorController, action='unassignNetwork',
                       conditions=dict(method=['PUT']))

