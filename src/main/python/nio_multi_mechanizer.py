# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0/LGPL 2.1
#
# The contents of this file are subject to the Mozilla Public License Version
# 1.1 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS" basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
# for the specific language governing rights and limitations under the
# License.
#
# The Original Code is Mozilla Bagheera.
#
# The Initial Developer of the Original Code is
# Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2011
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
# Xavier Stevens <xstevens@mozilla.com>
# Alternatively, the contents of this file may be used under the terms of
# either the GNU General Public License Version 2 or later (the "GPL"), or
# the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
# in which case the provisions of the GPL or the LGPL are applicable instead
# of those above. If you wish to allow use of your version of this file only
# under the terms of either the GPL or the LGPL, and not to allow others to
# use your version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the notice
# and other provisions required by the GPL or the LGPL. If you do not delete
# the provisions above, a recipient may use your version of this file under
# the terms of any one of the MPL, the GPL or the LGPL.
#
# ***** END LICENSE BLOCK *****
import mechanize
import uuid
import random
import time
import socket
import datapacket_pb2

class Transaction:
    
    def __init__(self):
        self.custom_timers = {}
        self.servers = ["localhost"]
        self.namespace = "metrics_ping"
        
    def generate_json(self):
        r = random.randint(10,50)
        s = "{ "
        for i in range(0,r):
            s += "metric%d: %d" % (i,i)
            if (i+1) < r:
                s += ", "
        s += " }"
        
        return s
    
    def run(self):
        data_packet = datapacket_pb2.DataPacket()
        data_packet.string_id = str(uuid.uuid4())
        data_packet.namespace = self.namespace
        data_packet.payload = self.generate_json()
        data_packet.payload_type = datapacket_pb2.DataPacket.JSON
        start_timer = time.time()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(data_packet.SerializeToString(), (random.choice(self.servers), 1575))
        sock.close()
        latency = time.time() - start_timer
        #self.custom_timers['response time'] = latency
        #timer_key = 'response time %d' % (response.code)
        self.custom_timers["udp"] = latency
        
if __name__ == '__main__':
    trans = Transaction()
    trans.run()
    
    for t in trans.custom_timers.iterkeys():
        print '%s: %.5f secs' % (t, trans.custom_timers[t])
