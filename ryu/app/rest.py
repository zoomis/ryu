# Copyright (C) 2011 Nippon Telegraph and Telephone Corporation.
# Copyright (C) 2011 Isaku Yamahata <yamahata at valinux co jp>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3 of the License
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import json
from ryu.exception import MacAddressAlreadyExist
from ryu.exception import NetworkNotFound, NetworkAlreadyExist
from ryu.exception import PortNotFound, PortAlreadyExist
from ryu.app.wsapi import WSPathComponent
from ryu.app.wsapi import WSPathExtractResult
from ryu.app.wsapi import WSPathStaticString
from ryu.app.wsapi import wsapi
from ryu.app.wspath import WSPathNetwork
from ryu.lib import mac

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
#
# get the list of mac addresses of dpid and port
# GET /v1.0/networks/{network-id}/{dpid}_{port-id}/macs/
#
# register a new mac address for dpid and port
# Fail if mac address is already registered or the mac address is used
# for other ports of the same network-id
# POST /v1.0/networks/{network-id}/{dpid}_{port-id}/macs/{mac}
#
# update a new mac address for dpid and port
# Success as nop even if same mac address is already registered.
# For now, changing mac address is not allows as it fails.
# PUT /v1.0/networks/{network-id}/{dpid}_{port-id}/macs/{mac}
#
# For now DELETE /v1.0/networks/{network-id}/{dpid}_{port-id}/macs/{mac}
# is not supported. mac address is released when port is deleted.
#


class WSPathPort(WSPathComponent):
    """ Match a {dpid}_{port-id} string """

    def __str__(self):
        return "{dpid}_{port-id}"

    def extract(self, pc, _data):
        if pc == None:
            return WSPathExtractResult(error="End of requested URI")

        try:
            dpid_str, port_str = pc.split('_')
            dpid = int(dpid_str, 16)
            port = int(port_str)
        except ValueError:
            return WSPathExtractResult(error="Invalid format: %s" % pc)

        return WSPathExtractResult(value={'dpid': dpid, 'port': port})


class WSPathMacAddress(WSPathComponent):
    """ Match a {mac} string: %02x:%02x:%02x:%02x:%02x:%02x
       Internal representation of mac address is string[6]"""

    def __str__(self):
        return "{mac}"

    def extract(self, pc, data):
        if pc == None:
            return WSPathExtractResult(error="End of requested URI")

        try:
            mac_addr = mac.haddr_to_bin(pc)
        except ValueError:
            return WSPathExtractResult(error="Invalid format: %s" % pc)

        return WSPathExtractResult(value=mac_addr)


class restapi:

    def __init__(self, *_args, **kwargs):
        self.ws = wsapi()
        self.api = self.ws.get_version("1.0")
        self.nw = kwargs['network']
        self.register()

    def list_networks_handler(self, request, _data):
        request.setHeader("Content-Type", 'application/json')
        return json.dumps(self.nw.list_networks())

    def create_network_handler(self, request, data):
        network_id = data['{network-id}']

        try:
            self.nw.create_network(network_id)
        except NetworkAlreadyExist:
            request.setResponseCode(409)

        return ""

    def update_network_handler(self, _request, data):
        network_id = data['{network-id}']
        self.nw.update_network(network_id)
        return ""

    def remove_network_handler(self, request, data):
        network_id = data['{network-id}']

        try:
            self.nw.remove_network(network_id)
        except NetworkNotFound:
            request.setResponseCode(404)

        return ""

    def list_ports_handler(self, request, data):
        network_id = data['{network-id}']

        try:
            body = json.dumps(self.nw.list_ports(network_id))
        except NetworkNotFound:
            body = ""
            request.setResponseCode(404)

        request.setHeader("Content-Type", 'application/json')
        return body

    @staticmethod
    def _get_param_port(data):
        return (data['{network-id}'],
                data['{dpid}_{port-id}']['dpid'],
                data['{dpid}_{port-id}']['port'])

    def create_port_handler(self, request, data):
        (network_id, dpid, port) = self._get_param_port(data)

        try:
            self.nw.create_port(network_id, dpid, port)
        except NetworkNotFound:
            request.setResponseCode(404)
        except PortAlreadyExist:
            request.setResponseCode(409)

        return ""

    def update_port_handler(self, request, data):
        (network_id, dpid, port) = self._get_param_port(data)

        try:
            self.nw.update_port(network_id, dpid, port)
        except NetworkNotFound:
            request.setResponseCode(404)

        return ""

    def remove_port_handler(self, request, data):
        (network_id, dpid, port) = self._get_param_port(data)

        try:
            self.nw.remove_port(network_id, dpid, port)
        except (NetworkNotFound, PortNotFound):
            request.setResponseCode(404)

        return ""

    def list_mac_handler(self, request, data):
        (_network_id, dpid, port_no) = self._get_param_port(data)

        try:
            body = json.dumps([mac.haddr_to_str(mac_addr) for mac_addr in
                               self.nw.list_mac(dpid, port_no)])
        except PortNotFound:
            request.setResponseCode(404)
            return ""

        request.setHeader("Content-Type", 'application/json')
        return body

    @staticmethod
    def _get_param_mac(data):
        return (data['{network-id}'],
                data['{dpid}_{port-id}']['dpid'],
                data['{dpid}_{port-id}']['port'],
                data['{mac}'])

    def create_mac_handler(self, request, data):
        (network_id, dpid, port_no, mac_addr) = self._get_param_mac(data)

        try:
            self.nw.create_mac(network_id, dpid, port_no, mac_addr)
        except MacAddressAlreadyExist:
            request.setResponseCode(409)
        except PortNotFound:
            request.setResponseCode(404)

        return ""

    def update_mac_handler(self, request, data):
        (network_id, dpid, port_no, mac_addr) = self._get_param_mac(data)

        try:
            self.nw.update_mac(network_id, dpid, port_no, mac_addr)
        except PortNotFound:
            request.setResponseCode(404)

        return ""

    def register(self):
        path_networks = (WSPathStaticString('networks'), )
        self.api.register_request(self.list_networks_handler, "GET",
                                  path_networks,
                                  "get the list of networks")

        path_network = path_networks + (WSPathNetwork(), )
        self.api.register_request(self.create_network_handler, "POST",
                                  path_network,
                                  "register a new network")

        self.api.register_request(self.update_network_handler, "PUT",
                                  path_network,
                                  "update a network")

        self.api.register_request(self.remove_network_handler, "DELETE",
                                  path_network,
                                  "remove a network")

        self.api.register_request(self.list_ports_handler, "GET",
                                  path_network,
                                  "get the list of sets of dpid and port")

        path_port = path_network + (WSPathPort(), )
        self.api.register_request(self.create_port_handler, "POST",
                                  path_port,
                                  "register a new set of dpid and port")

        self.api.register_request(self.update_port_handler, "PUT",
                                  path_port,
                                  "update a set of dpid and port")

        self.api.register_request(self.remove_port_handler, "DELETE",
                                  path_port,
                                  "remove a set of dpid and port")

        path_macs = path_port + (WSPathStaticString('macs'), )
        self.api.register_request(self.list_mac_handler, "GET",
                                  path_macs,
                                  "get the list of mac addresses")

        path_mac = path_macs + (WSPathMacAddress(), )
        self.api.register_request(self.create_mac_handler, "POST",
                                  path_mac,
                                  "register a new mac address for a port")

        self.api.register_request(self.update_mac_handler, "PUT",
                                  path_mac,
                                  "update a mac address for a port")
