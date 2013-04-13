# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (C) 2013, The SAVI Project.
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

import logging
import struct
import httplib
import json

from ryu.base import app_manager
from ryu.controller import mac_to_port
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, CONFIG_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_0
from ryu.lib.mac import haddr_to_str, ipaddr_to_str
from janus.network.of_controller.janus_of_consts import JANEVENTS, JANPORTREASONS
from janus.network.of_controller.event_contents import EventContents

LOG = logging.getLogger('ryu.app.ryu2janus')

class Ryu2JanusForwarding(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_0.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(Ryu2JanusForwarding, self).__init__(*args, **kwargs)
        self.mac_to_port = {}

        # Janus address
        self.host = '127.0.0.1'
        self.port = 8090
        self.url_prefix = '/v1/network'

    def _forward2Controller(self, method, url, body=None, headers=None):
        conn = httplib.HTTPConnection(self.host, self.port)
        conn.request(method, url, body, headers)
        res = conn.getresponse()
        print "\n"
        if res.status in (httplib.OK,
                          httplib.CREATED,
                          httplib.ACCEPTED,
                          httplib.NO_CONTENT):
            return res

        raise httplib.HTTPException(
            res, 'code %d reason %s' % (res.status, res.reason),
            res.getheaders(), res.read())


    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def _port_status_handler(self, ev):
        msg = ev.msg
        reason = msg.reason
        port_no = msg.desc.port_no

        ofproto = msg.datapath.ofproto
        if reason == ofproto.OFPPR_ADD:
            LOG.info("port added %s", port_no)
            reason_id = JANPORTREASONS.JAN_PORT_ADD
            method = 'POST'
        elif reason == ofproto.OFPPR_DELETE:
            LOG.info("port deleted %s", port_no)
            reason_id = JANPORTREASONS.JAN_PORT_DELETE
            method = 'PUT' # 'DELETE' doesn't support a body in the request
        elif reason == ofproto.OFPPR_MODIFY:
            LOG.info("port modified %s", port_no)
            reason_id = JANPORTREASONS.JAN_PORT_MODIFY
            method = 'PUT'
        else:
            LOG.info("Illegal port state %s %s", port_no, reason)
            LOG.info("UNKNOWN PORT STATUS REASON")
            raise

        port_status_url = '/of_event/%s' % JANEVENTS.JAN_EV_PORTSTATUS
        body = "{'datapath_id': %s, 'reason': %s, 'port': %s}" % (msg.datapath.id, reason_id, port_no)
        header = {"Content-Type": "application/json"}

        url = self.url_prefix + port_status_url
        LOG.info("FORWARDING PORT STATUS TO JANUS: body = %s", body)
        self._forward2Controller(method, url, body, header)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        #print "My packet in handler"
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto

        contents = EventContents()
        contents.set_dpid(datapath.id)
        contents.set_buff_id(msg.buffer_id)

        dl_dst, dl_src, _eth_type = struct.unpack_from('!6s6sH', buffer(msg.data), 0)
        contents.set_in_port(msg.in_port)
        contents.set_dl_dst(haddr_to_str(dl_dst))
        contents.set_dl_src(haddr_to_str(dl_src))
        contents.set_eth_type(_eth_type)

        if _eth_type == 0x806: # ARP
            HTYPE, PTYPE, HLEN, PLEN, OPER, SHA, SPA, THA, TPA = struct.unpack_from('!HHbbH6s4s6s4s', buffer(msg.data), 14)
            contents.set_arp_htype(HTYPE)
            contents.set_arp_ptype(PTYPE)
            contents.set_arp_hlen(HLEN)
            contents.set_arp_plen(PLEN)
            contents.set_arp_oper(OPER)

            contents.set_arp_sha(haddr_to_str(SHA))
            contents.set_arp_spa(ipaddr_to_str(SPA))
            contents.set_arp_tha(haddr_to_str(THA))
            contents.set_arp_tpa(ipaddr_to_str(TPA))

        packet_in_url = '/of_event/%s' % JANEVENTS.JAN_EV_PACKETIN
        method = 'POST'
        #body = "{'datapath_id': %s, 'buffer_id': %s, 'in_port': %s, 'dl_src': '%s', 'dl_dst': '%s'}" % (datapath.id, msg.buffer_id, in_port, haddr_to_str(src), haddr_to_str(dst))
        body = json.dumps(contents.getContents())
        header = {"Content-Type": "application/json"}

        url = self.url_prefix + packet_in_url
        LOG.info("FORWARDING PACKET TO JANUS: body = %s", body)
        self._forward2Controller(method, url, body, header)

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        msg = ev.msg
        dpid = msg.datapath_id
        ports = msg.ports

        features_reply_url = '/of_event/%s' % JANEVENTS.JAN_EV_FEATURESREPLY
        method = 'PUT'
        body = json.dumps({'datapath_id': dpid, 'ports': ports.keys()})
        header = {"Content-Type": "application/json"}

        url = self.url_prefix + features_reply_url
        LOG.info("FORWARDING FEATURES REPLY TO JANUS: body = %s", body)
        self._forward2Controller(method, url, body, header)

