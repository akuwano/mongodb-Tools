#!/usr/bin/env python
#-*- coding: utf-8 -*-

import pymongo
import mongodbutil

class MongoDBCommon(object):
    def __init__(self, ipaddr, port):
        """ Create a new MongoDB Instance """
        self.ipaddr = ipaddr
        self.port = port
        self.database = None
        self.connection = None
        self._make_connection()

    def _make_connection(self):
        try:
            self.connection = pymongo.Connection( self.ipaddr,
                self.port, slave_okay=True)
        except Exception, e:
            print "MongoDB Connection Error"
            print 'server: ' + self.ipaddr + ':' + self.port
            print str(e)

if __name__ == '__main__':
    pass
