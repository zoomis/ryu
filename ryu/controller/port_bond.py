# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright (C) 2012, The SAVI Project.
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
from ryu.exception import BondAlreadyExist, BondNetworkMismatch, BondDatpathMismatch, BondPortNotFound
from ryu.exception import PortNotFound

LOG = logging.getLogger('ryu.controller.port_bond')

# Similar to Network class, PortBond stores a list of (dpid, port) pairs
#   to indiate which ports are bonded together. All ports need to belong
#   to the same switch.
#
# PortBond can be used with or without Network-based segregation. If it
#   is being used with Network-based segregation, other restrictions
#   include the need for ports in a bond to belong to the same network,
#   and a port's network UUID should not be changeable if in a bond.
class PortBond(object):
    def __init__(self, nw=None):
        self.bonds = {} # Key = bond_id, Value = [(dpid, port), ...]
        self.bond2dpid = {} # Key = bond_id, Value = dpid
        self.portCount = {} # Key = bond_id, Value = # Ports in bond
        self.nextPortIdx = {} # Key = bond_id, Value = Index for the list
                              #  returned by self.bonds
        self.globalID = 0 # Global incremental counter
        self.nw = nw
        self.bond2net = {} # Key = bond_id, Value = Network UUID

    # Link PortBond object to a Network class object
    # Returns nothing
    def setNetworkObjHandle(self, nw):
        self.nw = nw

    # Returns next output port for a given bond, or -1 if bond is empty
    # Currently implements a simple round-robin
    def get_out_port(self, bond_id):
        port = -1
        if self.portCount[bond_id] > 0:
            self.nextPortIdx[bond_id] = (self.nextPortIdx[bond_id] + 1) % self.portCount[bond_id]

            # Use nextPort to index bonds
            dpid, port = self.bonds[bond_id][self.nextPortIdx[bond_id]]

        return port

    # Registers a new bond_id
    # Optional: Associate the bond with a network_id
    # Returns bond_id on success; Raises exception on error
    def create_bond(self, dpid, network_id=None):
        self.globalID += 1
        # Prepend dpid just for clarity on where the bond is; Could just use ID
        bond_id = hex(dpid) + "_" + str(self.globalID)
        
        if bond_id in self.bonds:
            raise BondAlreadyExist(bond_id=bond_id)

        self.bond2dpid[bond_id] = dpid
        self.bonds[bond_id] = []
        self.portCount[bond_id] = 0
        if self.nw:
            self.bond2net[bond_id] = network_id

        return bond_id

    # Deletes a bond, if it exists, given a bond_id
    # Returns nothing
    def delete_bond(self, bond_id):
        if bond_id in self.bonds:
            del self.bond2dpid[bond_id]
            del self.bonds[bond_id]
            del self.portCount[bond_id]
            del self.nextPortIdx[bond_id]

            if self.nw and bond_id in self.bond2net:
                del self.bond2net[bond_id]

    # Registers a (dpid, port) as part of a bond
    # Returns nothing on success; Raises exception on error
    def add_port(self, bond_id, dpid, port):
        if bond_id in self.bonds:
            if self.nw:
                # If Networks-based segregation is utilized, check if
                #   port being added belongs to same network
                if self.nw.get_network(dpid, port) != self.bond2net[bond_id]:
                    raise BondNetworkMismatch(bond_id=bond_id)

            # Check if bond belongs in same dpid as port
            if self.bond2dpid[bond_id] != dpid:
                raise BondDatpathMismatch(bond_id=bond_id, dpid=dpid)

            if (dpid, port) not in self.bonds[bond_id]:
                self.bonds[bond_id].append((dpid, port))
                self.portCount[bond_id] += 1
                if self.portCount[bond_id] == 1:
                    self.nextPortIdx[bond_id] = 0

    # De-registers a (dpid, port) from a bond
    # Returns nothing on success; Raises exception on error
    def del_port(self, bond_id, dpid, port):
        if bond_id in self.bonds:
            try:
                self.bonds[bond_id].remove((dpid, port))
                self.portCount[bond_id] -= 1
                if self.nextPortIdx[bond_id] == self.portCount[bond_id]:
                    self.nextPortIdx[bond_id] -= 1
            except ValueError:
                raise BondPortNotFound(dpid=dpid, port=port, bond_id=bond_id)

    # Returns bond_id given a (dpid, port) pair
    # Function doubles as an "is_port_bonded" boolean function
    #   Returns None if port is not bonded
    def get_bond_id(self, dpid, port):
        for bond_id, ports in self.bonds.items():
            if (dpid, port) in ports:
                return bond_id

        return None

    # Returns list of (dpid, port) pairs that belong in a given bond
    def ports_in_bond(self, bond_id):
        return self.bonds.get(bond_id, None)

    # Returns list of bond_id's that match the given dpid and network_id
    # If dpid or network_id not specified (e.g. None), it will be
    #   treated as a wildcard
    def list_bonds(self, dpid=None, network_id=None):
        bonds = set(self.bonds.keys())
        if dpid:
            bonds_dpid = set()
            for bond_id in bonds:
                if self.bond2dpid[bond_id] != dpid:
                    bonds_dpid.add(bond_id)

            bonds -= bonds_dpid

        if network_id:
            bonds_nw_id = set()
            for bond_id in bonds:
                if self.bond2net[bond_id] != network_id:
                    bonds_nw_id.add(bond_id)

            bonds -= bonds_nw_id

        return list(bonds)



