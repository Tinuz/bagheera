#!/usr/bin/env python
# encoding: utf-8

import sys
import getopt
import leveldb

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
            opts, args = getopt.getopt(argv[1:], "hvi:", ["help", "input="])
        except getopt.error, msg:
            raise Usage(msg)
    
        # option processing
        input = None
        for option, value in opts:
            if option == "-v":
                verbose = True
            if option in ("-h", "--help"):
                raise Usage(help_message)
            if option in ("-i", "--input"):
                input = value
        
        db = leveldb.LevelDB(input)
        delete_batch = leveldb.WriteBatch()
        for k,v in db.RangeIter(include_value=True):
            print "%s => %s" % (k,v)
            delete_batch.Delete(k)
        db.Write(delete_batch, sync=True)
    except Usage, err:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
        print >> sys.stderr, "\t for help use --help"
        return 2


if __name__ == "__main__":
    sys.exit(main())
