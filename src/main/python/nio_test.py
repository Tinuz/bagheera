#!/usr/bin/env python
# encoding: utf-8
"""
nio_test.py

Created by Xavier Stevens on 2011-09-23.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.
"""

import sys
import getopt
import socket
import uuid
import datapacket_pb2

help_message = '''
The help message goes here.
'''


class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg


def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "ho:n:", ["help", "output="])
        except getopt.error, msg:
            raise Usage(msg)
    
        # option processing
        namespace = None
        for option, value in opts:
            if option == "-n":
                namespace = value
            if option in ("-h", "--help"):
                raise Usage(help_message)
            if option in ("-o", "--output"):
                output = value
                
        data_packet = datapacket_pb2.DataPacket()
        data_packet.string_id = str(uuid.uuid4())
        data_packet.namespace = namespace
        data_packet.payload = "{ \"metric\": 1 }"
        data_packet.payload_type = datapacket_pb2.DataPacket.JSON
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(data_packet.SerializeToString(), ('localhost', 1575))
        sock.close()
        print "Data packet sent"
    except Usage, err:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
        print >> sys.stderr, "\t for help use --help"
        return 2


if __name__ == "__main__":
    sys.exit(main())
