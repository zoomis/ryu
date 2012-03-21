# Copyright (C) 2012 Nippon Telegraph and Telephone Corporation.
# Copyright (C) 2012 Isaku Yamahata <yamahata at valinux co jp>
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

import httplib
import json
import logging

import ryu.exception as ryu_exc
from ryu.app.wsapi import wsapi
from ryu.app.wsapi import WSPathStaticString
from ryu.app.wspath import DPID
from ryu.app.wspath import DPID_FMT
from ryu.app.wspath import NETWORK_ID
from ryu.app.wspath import PORT_NO
from ryu.app.wspath import WSPathInt
from ryu.app.wspath import WSPathNetwork
from ryu.app.wspath import WSPathPort
from ryu.app.wspath import WSPathSwitch


LOG = logging.getLogger(__name__)

# REST API for tunneling
#
# register GRE tunnel key of this network
# Fail if the key is already registered
# POST /v1.0/tunnels/gre/networks/{network-id}/key/{tunnel_key}
#
# register GRE tunnel key of this network
# Success as nop even if the same key is already registered
# PUT /v1.0/tunnels/gre/networks/{network-id}/key/{tunnel_key}
#
# return allocated GRE tunnel key of this network
# GET /v1.0/tunnels/gre/networks/{network-id}/key
#
# get the ports of dpid that are used for tunneling
# GET /v1.0/tunnels/gre/switches/{dpid}/ports
#
# get the dpid of the other end of tunnel
# GET /v1.0/tunnels/gre/switches/{dpid}/ports/{port-id}/
#
# register the dpid of the other end of tunnel
# Fail if the dpid is already registered
# POST /v1.0/tunnels/gre/switches/{dpid}/ports/{port-id}/{remote_dpip}
#
# register the dpid of the other end of tunnel
# Success as nop even if the dpid is already registered
# PUT /v1.0/tunnels/gre/switches/{dpid}/ports/{port-id}/{remote_dpip}


REMOTE_DPID = '{remote-dpid}'
TUNNEL_KEY = '{tunnel-key}'


class WSPathTunnelKey(WSPathInt):
    """ Match a tunnel key value = 32 bit unsigned value """
    _name = TUNNEL_KEY
    _base = 16
    _max_value = 0xffffffff


class GRETunnelController(object):
    def __init__(self, *_args, **kwargs):
        super(GRETunnelController, self).__init__()
        self.nw = kwargs['network']
        self.tunnels = kwargs['tunnels']

        self.ws = wsapi()
        self.api = self.ws.get_version('1.0')
        self._register()

    @staticmethod
    def _get_param_network_id(data):
        return data[NETWORK_ID]

    @staticmethod
    def _get_param_tunnel_key(data):
        return (data[NETWORK_ID], data[TUNNEL_KEY])

    @staticmethod
    def _get_param_dpid(data):
        return data[DPID]

    @staticmethod
    def _get_param_port(data):
        return (data[DPID], data[PORT_NO])

    @staticmethod
    def _get_param_remote_dpid(data):
        return (data[DPID], data[PORT_NO], data[REMOTE_DPID])

    def get_key_handler(self, request, data):
        network_id = self._get_param_network_id(data)
        try:
            tunnel_key = self.tunnels.get_key(network_id)
        except ryu_exc.TunnelKeyNotFound:
            request.setResponseCode(httplib.NOT_FOUND)
            return 'no key found for network %s' % network_id

        request.setHeader('Content-Type', 'application/json')
        return json.dumps(tunnel_key)

    def delete_key_handler(self, request, data):
        network_id = self._get_param_network_id(data)
        try:
            self.tunnels.delete_key(network_id)
        except (ryu_exc.NetworkNotFound, ryu_exc.TunnelKeyNotFound):
            request.setResponseCode(httplib.NOT_FOUND)
            return 'no key found for network %s' % network_id
        return ''

    def create_key_handler(self, request, data):
        network_id, tunnel_key = self._get_param_tunnel_key(data)
        try:
            self.tunnels.register_key(network_id, tunnel_key)
        except (ryu_exc.NetworkAlreadyExist,
                ryu_exc.TunnelKeyAlreadyExist) as e:
            request.setResponseCode(httplib.CONFLICT)
            return str(e)
        return ''

    def update_key_handler(self, request, data):
        network_id, tunnel_key = self._get_param_tunnel_key(data)
        try:
            self.tunnels.update_key(network_id, tunnel_key)
        except (ryu_exc.NetworkAlreadyExist,
                ryu_exc.TunnelKeyAlreadyExist) as e:
            request.setResponseCode(httplib.CONFLICT)
            return str(e)
        return ''

    def list_tunnel_ports_handler(self, request, data):
        dpid = self._get_param_dpid(data)
        ports = self.tunnels.list_ports(dpid)

        request.setHeader('Content-Type', 'application/json')
        return json.dumps(ports)

    def delete_port_handler(self, request, data):
        dpid, port_no = self._get_param_port(data)
        try:
            self.tunnels.delete_port(dpid, port_no)
        except ryu_exc.PortNotFound as e:
            request.setResponseCode(httplib.NOT_FOUND)
            return str(e)
        return ''

    def get_remote_dpid_handler(self, request, data):
        dpid, port_no = self._get_param_port(data)
        try:
            remote_dpid = self.tunnels.get_remote_dpid(dpid, port_no)
        except ryu_exc.PortNotFound as e:
            request.setResponseCode(httplib.NOT_FOUND)
            return str(e)

        request.setHeader('Content-Type', 'application/json')
        return json.dumps(DPID_FMT % remote_dpid)

    def create_remote_dpid_handler(self, request, data):
        dpid, port_no, remote_dpid = self._get_param_remote_dpid(data)
        try:
            self.tunnels.register_port(dpid, port_no, remote_dpid)
        except ryu_exc.PortAlreadyExist as e:
            request.setResponseCode(httplib.CONFLICT)
            return str(e)
        return ''

    def update_remote_dpid_handler(self, request, data):
        dpid, port_no, remote_dpid = self._get_param_remote_dpid(data)
        try:
            self.tunnels.update_port(dpid, port_no, remote_dpid)
        except ryu_exc.RemoteDPIDAlreadyExist as e:
            request.setResponseCode(httplib.CONFLICT)
            return str(e)
        return ''

    def _register(self):
        path_gre = (WSPathStaticString('tunnels'), WSPathStaticString('gre'))

        path_key = path_gre + (WSPathStaticString('networks'),
                               WSPathNetwork(), WSPathStaticString('key'))
        self.api.register_request(self.get_key_handler, 'GET',
                                  path_key,
                                  'get the tunnel key of a network')
        self.api.register_request(self.delete_key_handler, 'DELETE',
                                  path_key,
                                  'delete the tunnel key of a network')

        path_tunnel_key = path_key + (WSPathTunnelKey(), )
        self.api.register_request(self.create_key_handler, 'POST',
                                  path_tunnel_key,
                                  'create the tunnel key of a network')
        self.api.register_request(self.update_key_handler, 'PUT',
                                  path_tunnel_key,
                                  'update the tunnel key of a network')

        path_switches = path_gre + (WSPathStaticString('switches'),
                                    WSPathSwitch(DPID))
        path_ports = path_switches + (WSPathStaticString('ports'), )
        self.api.register_request(self.list_tunnel_ports_handler, 'GET',
                                  path_ports,
                                  'get the list of ports used for tunnel')

        path_port = path_ports + (WSPathPort(), )
        self.api.register_request(self.delete_port_handler, 'DELETE',
                                  path_port,
                                  'delete the tunnel port')
        self.api.register_request(self.get_remote_dpid_handler, 'GET',
                                  path_port,
                                  'get the remote dpid')

        path_remote_dpid = path_port + (WSPathSwitch(REMOTE_DPID), )
        self.api.register_request(self.create_remote_dpid_handler, 'POST',
                                  path_remote_dpid,
                                  'register remote dpid')
        self.api.register_request(self.update_remote_dpid_handler, 'PUT',
                                  path_remote_dpid,
                                  'update remote dpid')
