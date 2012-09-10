import logging
import gflags

from ryu.exception import NetworkNotFound, NetworkAlreadyExist
from ryu.exception import PortAlreadyExist, PortNotFound, PortUnknown
from ryu.app.rest_nw_id import NW_ID_UNKNOWN, NW_ID_EXTERNAL
from subprocess import Popen, PIPE, STDOUT

LOG = logging.getLogger('ryu.controller.flowvisor_cli')

FLAGS = gflags.FLAGS
gflags.DEFINE_string('fv_pass_file', '/usr/local/etc/flowvisor/passFile',
                                        'FlowVisor control password file')
gflags.DEFINE_string('fv_slice_default_pass', 'supersecret',
                      'FlowVisor non-admin slice default password')
gflags.DEFINE_string('fv_default_slice', 'fvadmin',
                      'FlowVisor default slice name')

class FlowVisor_CLI(object):
    def __init__(self):
        self.flowspace_ids = {} # Dictionary of {(dpid, port, mac) : [flowspace id]}
        self.slice2network = {} # Dictionary of {sliceName : [network_ids]}
        self.cmdPrefix = "fvctl --passwd-file=" + FLAGS.fv_pass_file + " "
        self.defaultSlice = FLAGS.fv_default_slice

    def listSlices(self):
        cmdLine = "listSlices"
        p = Popen(self.cmdPrefix + cmdLine, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
        out, err = p.communicate()
        return out

    def createSlice(self, sliceName, ip, port):
        # Use a garbage email address...
        cmdLine = "createSlice " + sliceName + " tcp:" + ip + ":" + port + " blek@blek.ca"
        p = Popen(self.cmdPrefix + cmdLine, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
        out, err = p.communicate(FLAGS.fv_slice_default_pass)
        return out[14:]

    def deleteSlice(self, sliceName):
        cmdLine = "deleteSlice " + sliceName
        p = Popen(self.cmdPrefix + cmdLine, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
        out, err = p.communicate()
        return out

    # srcMAC is to be specified in hexadecimal notation and byte-separated by colons
    def addFlowSpace(self, sliceName, dpid, port, srcMAC):
        # Priority of 100 picked randomly...
        cmdLine = "addFlowSpace " + hex(dpid)[2:-1] + " 100 in_port=" + str(port) + ",dl_src=" + srcMAC + " Slice:" + sliceName + "=4"
        p = Popen(self.cmdPrefix + cmdLine, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
        out, err = p.communicate()
        return out

    def removeFlowSpace(self, flowspace_id):
        cmdLine = "removeFlowSpace " + str(flowspace_id)
        p = Popen(self.cmdPrefix + cmdLine, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
        out, err = p.communicate()
        return out

    # ==================================================================
    # The functions below are helper functions that are not CLIs
    # ==================================================================

    def addFlowSpaceID(self, dpid, port, mac, flowspace_id):
        self.flowspace_ids[(dpid, port, mac)] = flowspace_id

    def delFlowSpaceIDs(self, idList):
        # idList can also be a single integer; Convert to list
        if type(idList) is int:
            idList = [idList]
        
        for tuple, id in self.flowspace_ids.items():
            if id in idList:
                try:
                    del self.flowspace_ids[tuple]
                except KeyError:
                    # How to handle such an error?
                    pass

    # Returns a list of FlowSpace IDs whose tuple matches the input parameters
    # Use 'None' as a wildcard
    def getFlowSpaceIDs(self, dpid=None, port=None, mac=None):
        idList = []

        for tuple, id in self.flowspace_ids.items():
            dpid_match = (dpid is None) or (dpid in tuple)
            port_match = (port is None) or (port in tuple)
            mac_match = (mac is None) or (mac in tuple)

            if (dpid_match and port_match and mac_match):
                idList.append(id)

        return idList

    def slice2nw_add(self, sliceName, network_id):
        self.slice2network.setdefault(sliceName, [])
        self.slice2network[sliceName].append(network_id)

    def slice2nw_del(self, network_id):
        for slice, nw_ids in self.slice2network.items():
            if (network_id in nw_ids):
                self.slice2network[slice].remove(network_id)
                break

    # Returns sliceName that network_id is delegated to, if it is delgated
    # Otherwise, returns None
    def getSliceName(self, network_id):
        if network_id:
            for slice, nw_ids in self.slice2network.items():
                if (network_id in nw_ids):
                    return slice

        return None

    # Called when PortController wants to create or update a port
    def updatePort(self, network_id, dpid, port, portExists, old_network_id=None):
        if portExists: # Updating an existing port whose network has changed
            self.deletePort(old_network_id, dpid, port)

        # Leave adding new FlowSpace IDs to application's packet handler

    # Called when PortController wants to delete a port
    def deletePort(self, network_id, dpid, port):
        if self.getSliceName(network_id) or (network_id == NW_ID_EXTERNAL):
            try:
                flowspace_ids = self.getFlowSpaceIDs(dpid, port)
            except PortUnknown:
                raise PortNotFound(dpid=dpid, port=port, network_id=network_id)
            else:
                for id in flowspace_ids:
                    ret = self.removeFlowSpace(id)
                    # To Do: Error check on ret

                self.delFlowSpaceIDs(flowspace_ids)
            
            # Should we delete entry from the switch's flow table?
            # Or should we assume the application will take care of it?

