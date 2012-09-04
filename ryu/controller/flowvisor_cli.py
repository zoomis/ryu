import logging
import gflags

from ryu.exception import NetworkNotFound, NetworkAlreadyExist
from ryu.exception import PortAlreadyExist, PortNotFound, PortUnknown
from subprocess import Popen, PIPE, STDOUT

LOG = logging.getLogger('ryu.controller.flowvisor_cli')

FLAGS = gflags.FLAGS
gflags.DEFINE_string('fv_pass_file', '/usr/local/etc/flowvisor/passFile',
                                        'FlowVisor control password file')
gflags.DEFINE_string('fv_slice_default_pass', 'supersecret',
                      'FlowVisor non-admin slice default password')

class FlowVisor_CLI(object):
    def __init__(self):
        self.flowspace_ids = {} # Dictionary of {(dpid, port) : [flowspace ids]}
        self.slice2network = {} # Dictionary of {sliceName : [network_ids]}
        self.cmdPrefix = "fvctl --passwd-file=" + FLAGS.fv_pass_file + " "

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

    def addFlowSpace(self, sliceName, dpid, port):
        # Priority of 100 picked randomly...
        cmdLine = "addFlowSpace " + hex(dpid)[2:-1] + " 100 in_port=" + str(port) + " Slice:" + sliceName + "=4"
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

    def addFlowSpaceID(self, dpid, port, flowspace_id):
        self.flowspace_ids[(dpid, port)] = flowspace_id

    def delFlowSpaceID(self, dpid, port):
        try:
            del self.flowspace_ids[(dpid, port)]
        except KeyError:
            raise PortUnknown(dpid=dpid, port=port)

    def getFlowSpaceID(self, dpid, port):
        try:
            id = self.flowspace_ids[(dpid, port)]
        except KeyError:
            raise PortUnknown(dpid=dpid, port=port)
        else:
            return id

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
        if portExists: # Updating an existing port
            if self.getSliceName(old_network_id):
                # Must delete old FlowSpace rule for this port from old network
                try:
                    old_flowspace_id = self.getFlowSpaceID(dpid, port)
                except PortUnknown:
                    raise
                else:
                    self.removeFlowSpace(old_flowspace_id)

        sliceName = self.getSliceName(network_id)
        if sliceName:
            # Must install FlowSpace rule for this new port in the network
            ret = self.addFlowSpace(sliceName, dpid, port)
            if (ret.find("success") < 0):
                # Error occured while attempting to install FV rule
                # TO DO: What exception to pass back to caller??
                # status = 500
                raise

            # Keep track of installed rules related to network
            self.addFlowSpaceID(dpid, port, ret[9:])

    # Called when PortController wants to delete a port
    def deletePort(self, network_id, dpid, port):
        if self.getSliceName(network_id):
            try:
                flowspace_id = self.getFlowSpaceID(dpid, port)
            except PortUnknown:
                raise PortNotFound(dpid=dpid, port=port, network_id=network_id)
            else:
                self.removeFlowSpace(flowspace_id)
                self.delFlowSpaceID(dpid, port)
            
            # Should we delete entry from the switch's flow table?
            # Or should we assume the application will take care of it?

