# Copyright (C) 2011 Nippon Telegraph and Telephone Corporation.
# Copyright (C) 2011 Isaku Yamahata <yamahata at valinux co jp>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


class RyuException(Exception):
    message = 'An unknown exception'

    def __init__(self, msg=None, **kwargs):
        self.kwargs = kwargs
        if msg is None:
            msg = self.message

        try:
            msg = msg % kwargs
        except Exception:
            msg = self.message

        super(RyuException, self).__init__(msg)


class OFPUnknownVersion(RyuException):
    message = 'unknown version %(version)x'


class OFPMalformedMessage(RyuException):
    message = 'malformed message'


class NetworkNotFound(RyuException):
    message = 'no such network id %(network_id)s'


class NetworkAlreadyExist(RyuException):
    message = 'network id %(network_id)s already exists'


class PortNotFound(RyuException):
    message = 'no such port (%(dpid)s, %(port)s) in network %(network_id)s'


class PortAlreadyExist(RyuException):
    message = 'port (%(dpid)s, %(port)s) in network %(network_id)s ' \
              'already exists'


class PortUnknown(RyuException):
    message = 'unknown network id for port (%(dpid)s %(port)s)'


class MacAddressAlreadyExist(RyuException):
    message = 'port (%(dpid)s, %(port)s) has already mac %(mac_address)s'


class MacAddressDuplicated(RyuException):
    message = 'MAC address %(mac)s is duplicated'


class RemoteDPIDAlreadyExist(RyuException):
    message = ('port (%(dpid)s, %(port)s) has already '
               'remote dpid %(remote_dpid)s')


class TunnelKeyAlreadyExist(RyuException):
    message = 'tunnel key %(tunnel_key)s already exists'


class TunnelKeyNotFound(RyuException):
    message = 'no tunnel key for network %(network_id)s'
