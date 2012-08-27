import logging

from ryu.exception import NetworkNotFound, NetworkAlreadyExist
from ryu.exception import PortAlreadyExist, PortNotFound, PortUnknown
from subprocess import Popen, PIPE, STDOUT

LOG = logging.getLogger('ryu.controller.flowvisor_cli')

# The password file name and location is dependent on individual installation...
# Any way to get around?
cmdPrefix = "fvctl --passwd-file=/usr/local/etc/flowvisor/passFile "

class FlowVisor_CLI(object):
    def __init__(self):
        #self.slices = {}
        pass

    def listSlices(self):
        cmdLine = "listSlices"
        p = Popen(cmdPrefix + cmdLine, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
        out, err = p.communicate()
        return out

    def createSlice(self, sliceName, ip, port, pwd):
        # Use a garbage email address...
        cmdLine = "createSlice " + sliceName + " tcp:" + ip + ":" + port + " blek@blek.ca"
        p = Popen(cmdPrefix + cmdLine, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
        out, err = p.communicate(pwd)
        return out[14:]

    def deleteSlice(self, sliceName):
        cmdLine = "deleteSlice " + sliceName
        p = Popen(cmdPrefix + cmdLine, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
        out, err = p.communicate()
        return out

    def routeToController(self, sliceName, dpid, port):
        # Priority of 100 picked randomly...
        cmdLine = "addFlowSpace " + hex(dpid)[2:-1] + " 100 in_port=" + str(port) + " Slice:" + sliceName + "=4"
        p = Popen(cmdPrefix + cmdLine, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
        out, err = p.communicate()
        return out

