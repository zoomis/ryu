# Copyright (C) 2011 Nippon Telegraph and Telephone Corporation.
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

from ryu.base import app_manager
from ryu.controller import mac_to_port
from ryu.controller import ofp_event
from ryu.controller import dpset
from ryu.controller.handler import MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import nx_match
from ryu.lib.mac import haddr_to_str


LOG = logging.getLogger('ryu.app.configurable_device')

# TODO: we should split the handler into two parts, protocol
# independent and dependant parts.

# TODO: can we use dpkt python library?

# TODO: we need to move the followings to something like db


class ConfigurableDevice(app_manager.RyuApp):
    _CONTEXTS = {
        'mac2port': mac_to_port.MacToPortTable,
        'dpset': dpset.DPSet
        }

    def __init__(self, *args, **kwargs):
        super(ConfigurableDevice, self).__init__(*args, **kwargs)
        self.nw = kwargs['network']
        self.mac2port = kwargs['mac2port']
        self.dpset = kwargs['dpset']
        self.packetHandlers = {0 : self.hub_configuration,
                               1 : self.switch_configuration}
        self.currHandler = self.packetHandlers[0] # Function pointer
        self.handler_id = 0; # Identifies current handler
        self.nw.packetHandlerCallback = self.changePacketHandler;
    
    # Callback function (called from network)
    def changePacketHandler(self, new_handler_id):
        if (new_handler_id != self.handler_id):
            newHandlerFunc = self.packetHandlers.get(new_handler_id, None)

            if newHandlerFunc == None:
                print "Unknown packet handler ID set; Using last known handler"
            else:
                print "Changing packet handler to handler ID " + str(new_handler_id)
                self.handler_id = new_handler_id
                self.currHandler = newHandlerFunc
                
                # Delete flows set by previous handler
                print "Deleting all flows set by previous handler"
                for dpid, dp in self.dpset.get_all():
                    dp.send_delete_all_flows()
        else:
            print "Already using the packet handler with ID " + str(new_handler_id)

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        # Handle packet
        self.currHandler(ev)
        
        # Print new line between each packet info
        print "\n"

    # Device acts as an L2 learning switch
    def switch_configuration(self, ev):
        print "Packet in: switch configuration"
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto

        dst, src, _eth_type = struct.unpack_from('!6s6sH', buffer(msg.data), 0)

        dpid = datapath.id
        self.mac2port.dpid_add(dpid)
        LOG.info("Src MAC: %s; Dest MAC: %s", haddr_to_str(src), haddr_to_str(dst))

        self.mac2port.port_add(dpid, msg.in_port, src)
        out_port = self.mac2port.port_get(dpid, dst)

        if out_port == None:
            LOG.info("Output port not found")
            out_port = ofproto.OFPP_FLOOD

        actions = [datapath.ofproto_parser.OFPActionOutput(out_port)]
        LOG.info("Input port: %s; Output port: %s", msg.in_port, out_port)

        if out_port != ofproto.OFPP_FLOOD:
            rule = nx_match.ClsRule()
            rule.set_in_port(msg.in_port)
            rule.set_dl_dst(dst)
            rule.set_dl_src(src)
            rule.set_nw_dscp(0)
            datapath.send_flow_mod(
                rule=rule, cookie=0, command=ofproto.OFPFC_ADD,
                idle_timeout=0, hard_timeout=0,
                priority=ofproto.OFP_DEFAULT_PRIORITY,
                flags=ofproto.OFPFF_SEND_FLOW_REM, actions=actions)

        datapath.send_packet_out(msg.buffer_id, msg.in_port, actions)

    # Device acts as a hub
    def hub_configuration(self, ev):
        print "Packet in: hub configuration"
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto

        dst, src, _eth_type = struct.unpack_from('!6s6sH', buffer(msg.data), 0)

        dpid = datapath.id
        LOG.info("Src MAC: %s; Dest MAC: %s", haddr_to_str(src), haddr_to_str(dst))

        actions = [datapath.ofproto_parser.OFPActionOutput(ofproto.OFPP_FLOOD)]
        LOG.info("Input port: %s; Output port: %s", msg.in_port, ofproto.OFPP_FLOOD)

        rule = nx_match.ClsRule()
        rule.set_in_port(msg.in_port)
        rule.set_dl_dst(dst)
        rule.set_dl_src(src)
        rule.set_nw_dscp(0)
        datapath.send_flow_mod(
            rule=rule, cookie=0, command=ofproto.OFPFC_ADD,
            idle_timeout=0, hard_timeout=0,
            priority=ofproto.OFP_DEFAULT_PRIORITY,
            flags=ofproto.OFPFF_SEND_FLOW_REM, actions=actions)

        datapath.send_packet_out(msg.buffer_id, msg.in_port, actions)

    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def _port_status_handler(self, ev):
        msg = ev.msg
        reason = msg.reason
        port_no = msg.desc.port_no

        ofproto = msg.datapath.ofproto
        if reason == ofproto.OFPPR_ADD:
            LOG.info("port added %s", port_no)
        elif reason == ofproto.OFPPR_DELETE:
            LOG.info("port deleted %s", port_no)
        elif reason == ofproto.OFPPR_MODIFY:
            LOG.info("port modified %s", port_no)
        else:
            LOG.info("Illeagal port state %s %s", port_no, reason)
