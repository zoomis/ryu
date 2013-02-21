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

import httplib
import json
import logging
import time

from ryu.app.wsapi import wsapi
from ryu.app.wsapi import WSPathStaticString
from ryu.app.wspath import DPID
from ryu.app.wspath import WSPathSwitch
from ryu.base import app_manager
from ryu.controller import dpset
from ryu.controller import link_set
from ryu.lib import dpid as lib_dpid


LOG = logging.getLogger(__name__)


# REST API for discovery status
#
# get all the links
# GET /v1.0/topology/links
#
# get the links connected <dpid>
# GET /v1.0/topology/switch/dpid>/links
#
# where
# <dpid>: datapath id in 16 hex


class DiscoveryController(app_manager.RyuApp):
    _CONTEXTS = {
        'dpset': dpset.DPSet,
        'link_set': link_set.LinkSet,
        }

    def __init__(self, *args, **kwargs):
        super(DiscoveryController, self).__init__(*args, **kwargs)
        self.dpset = kwargs['dpset']
        self.link_set = kwargs['link_set']

        self.ws = wsapi()
        self.api = self.ws.get_version('1.0')
        self._register()

    @staticmethod
    def _format_link(link, timestamp, now):
        return {
            'timestamp': now - timestamp,
            'dp1': lib_dpid.dpid_to_str(link.src.dpid),
            'port1': link.src.port_no,
            'dp2': lib_dpid.dpid_to_str(link.dst.dpid),
            'port2': link.dst.port_no,
        }

    def _format_response(self, iteritems):
        now = time.time()
        response = {
            'identifier': 'name',
            'items': [self._format_link(link, ts, now)
                      for link, ts in iteritems],
        }
        return json.dumps(response)

    def get_links(self, request, _data):
        request.setHeader('Content-Type', 'application/json')
        return self._format_response(self.link_set.get_items())

    def get_switch_links(self, request, data):
        request.setHeader('Content-Type', 'application/json')
        dpid = data[DPID]
        if self.dpset.get(dpid) is None:
            request.setResponseCode(httplib.NOT_FOUND)
            return 'dpid %s is not founf' % dpid

        return self._format_response(self.link_set.get_items(dpid))

    def _register(self):
        path_topology = (WSPathStaticString('topology'), )

        path_links = path_topology + (WSPathStaticString('links'), )
        self.api.register_request(self.get_links, 'GET', path_links,
                                  'Get list of links.')

        path_switch_links = path_topology + (WSPathStaticString('switch'),
                                             WSPathSwitch(DPID),
                                             WSPathStaticString('links'))
        self.api.register_request(self.get_switch_links, 'GET',
                                  path_switch_links, 'Get list of links.')
