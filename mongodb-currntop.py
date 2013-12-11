#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
import logging
import re
import json
import argparse
import os
#import traceback
#import csv
#import pymongo


# MyLibrary
import mongodbutil
from mongodbutil.common import *
from mongodbutil.command import *

class ArgumentParse(object):
    """
    Parse Argument.

    """
    def __init__(self):
        try:
            parser = argparse.ArgumentParser(description='mongodb-tools')
            parser.add_argument('-D', action="store_true", dest="DEBUG"
                ,help="DEBUG Option", default=False)
            parser.add_argument('-p', action="store", dest="priority"
                , help="ReplicaSets Priority" , default="1")
            parser.add_argument('-f', action="store", dest="serverlist"
                , help="Server List File" , default="./server.list")
            parser.add_argument('-r', action="store", dest="replicasets"
                , help="ReplcaSets" , default="replSets")
            self.optargs = parser.parse_args()
            self.checkopts()
        except Exception, e:
            print >>sys.stderr, "Argument Parse Error"
            print >>sys.stderr, str(e)
            sys.exit(1)

    def checkopts(self):
        if not self.optargs.serverlist or os.path.isfile(self.optargs.serverlist) is False:
            print("Command option error. --file -f : %s" % (self.optargs.serverlist))
            return False

def main():
    ### print "---- INIT ----"
    servers = {}

    ### print "---- ARGUMENT PARSE ----"
    arguparse = ArgumentParse()

    ### print "---- DEBUG CHECK ----"
    if arguparse.optargs.DEBUG:
        logging.basicConfig(level=logging.DEBUG)

    ### print "---- CONNECT MONGODB ----"
    fp = open(arguparse.optargs.serverlist, 'r')
    for line in fp:
        line = line.rstrip()
        comment_pattern = re.compile('^#')
        comment_match = comment_pattern.match(line)
        if comment_match:
            continue
        server = line.split(':')
        servers[line] = mongodbutil.common.MongoDBCommon(str(server[0]),
                int(server[1]))


    ### print "---- GET DATA MONGODB ----"
    runcommand = mongodbutil.command.RunMongoDBCommand(arguparse.optargs,
            'CurrentOpList', servers)

if __name__ == '__main__':
    main()








