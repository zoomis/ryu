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

# Internal representation of mac address is string[6]
_HADDR_LEN = 6

DONTCARE = '\x00' * 6
BROADCAST = '\xff' * 6
MULTICAST = '\xfe' + '\xff' * 5
UNICAST = '\x01' + '\x00' * 5


def is_multicast(addr):
    return bool(ord(addr[0]) & 0x01)


def haddr_to_str(addr):
    """Format mac address in internal representation into human readable
    form"""
    assert len(addr) == _HADDR_LEN
    # [:-1] is to remove trailing ':'
    return ''.join(['%02x:' % ord(char) for char in addr])[:-1]


def haddr_to_bin(string):
    """Parse mac address string in human readable format into
    internal representation"""
    hexes = string.split(':')
    if len(hexes) != _HADDR_LEN:
        ValueError('Invalid format for mac address: %s' % string)
    return ''.join([chr(int(h, 16)) for h in hexes])
