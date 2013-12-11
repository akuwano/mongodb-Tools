#/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import logging
import pymongo
import bson
import mongodbutil

class RunMongoDBCommand(object):
    def __init__(self, command, commandname, servers):
        """ Create a new MongoDB Instance """
        self.command = command
        self.servers = servers
        self.commandname = commandname
        # Command List Dictionary
        self.commandslist = {
            "ReplicaSetsList": mongodbutil.command.ReplicaSetsList,
            "MasterList": mongodbutil.command.MasterList,
            "MongodConnCount": mongodbutil.command.MongodConnCount,
            "PrioritySearch": mongodbutil.command.PrioritySearch,
            "FsyncStop": mongodbutil.command.FsyncStop,
            "FsyncStart": mongodbutil.command.FsyncStart,
            "BalancerStop": mongodbutil.command.BalancerStop,
            "BalancerStart": mongodbutil.command.BalancerStart,
            "BSONDump": mongodbutil.command.BSONDump,
            "DelayAdd": mongodbutil.command.DelayAdd,
            "ShardCollectionList": mongodbutil.command.ShardCollectionList,
            "CurrentOpList": mongodbutil.command.CurrentOpList,
            "TEST": mongodbutil.command.TEST
        }
        self.run_command_select()

    def run_command_select(self):
        try:
            # DebugLog
            logging.debug("COMMAND EXEC START:")
            logging.debug(self.command)
            logging.debug(self.servers)
            logging.debug(type(self.servers))

            selectcommand = self.commandslist[self.commandname]
            if selectcommand:
                self.runcommand = selectcommand(self.servers, self.command)
            else:
                print "NotFound Command: " + self.command
        except Exception, e:
            print "MongoDB Command Error"
            print str(e)
            sys.exit(1)

class Command(object):
    """
        Command Basic Class.
    """
    def __init__(self, servers, command):
        """ Execute MongoDB TEST Command Init """
        self.servers = servers
        self.command = command
        self.response = {}
        self.result = {}
        self.result['nonreplicasets'] = []
        self.run_command()
        self.output_result()

    def run_command(self):
        pass

    def output_result(self):
        pass

class ReplicaSetsList(Command):
    """
        ReplicaSetsList Command Class.
    """
    def run_command(self):
        try:
            for server in self.servers.keys():
                # DebugLog
                logging.debug(server)
                try:
                    self.response = self.servers[server].connection.admin.command("replSetGetStatus")
                except Exception, e:
                    self.result['nonreplicasets'].append(server)
                    pass
                logging.debug(type(self.response))
                for i in self.response['members']:
                    logging.debug(i['name'])
                    logging.debug(self.response['set'])
                    # init self.result[] or dict append.

                    # when No exist Key , key Generate.
                    if self.result.has_key(str(self.response['set'])) != True:
                        self.result[str(self.response['set'])] = []
                    if not i['name'] in self.result[str(self.response['set'])]:
                        self.result[str(self.response['set'])].append(i['name'])
                        logging.debug(self.response['set'])
                    logging.debug(self.result[str(self.response['set'])])
                # print "UNIQ"
                for i in self.result.keys():
                    self.result[i] = list(set(self.result[i]))
            logging.debug("LAST")
            logging.debug(self.result)
        except Exception, e:
            pass
            print "MongoDB TEST Command Error"
            print str(e)
            sys.exit(1)

    def output_result(self):
        template = "{0:20}  {1:>30}"
        tmpi =  ""
        for i in sorted(self.result.keys()):
            self.result[i].sort()
            for j in self.result[i]:
                if  tmpi == i:
                    print template.format("", j)
                else:
                    print template.format(i, j)
                    tmpi = i

class MasterList(Command):
    """
        MasterList Command Class.
    """
    def run_command(self):
        try:
            for server in self.servers.keys():
                logging.debug(server)
                try:
                    self.response = self.servers[server].connection.admin.command("replSetGetStatus")
                except Exception, e:
                    self.result['nonreplicasets'].append(server)
                    pass
                logging.debug(type(self.response))
                for i in self.response['members']:
                    logging.debug(i['name'])
                    logging.debug(self.response['set'])
                    # init self.result[] or dict append.

                    # when No exist Key , key Generate.
                    if self.result.has_key(str(self.response['set'])) != True:
                        self.result[str(self.response['set'])] = []

                    if not i['name'] in self.result[str(self.response['set'])]:
                        if "PRIMARY"  in i['stateStr']:
                            self.result[str(self.response['set'])].append(i['name'])
                        logging.debug(self.response['set'])
                    logging.debug(self.result[str(self.response['set'])])
                # print "UNIQ"
                for i in self.result.keys():
                    self.result[i] = list(set(self.result[i]))
            logging.debug("LAST")
            logging.debug(self.result)
        except Exception, e:
            pass
            print "MongoDB MasterList Command Error"
            print str(e)
            sys.exit(1)

    def output_result(self):
        for i in sorted(self.result.keys()):
            self.result[i].sort()
            for j in self.result[i]:
                print j

class MongodConnCount(Command):
    """
        MongodConnCount Command Class.
    """
    def run_command(self):
        try:
            for server in self.servers.keys():
                logging.debug(server)
                try:
                    self.response = \
                        self.servers[server].connection.admin.command("serverStatus")
                except Exception, e:
                    self.result['nonreplicasets'].append(server)
                    pass
                # init self.result[] or dict append.
                # when No exist Key , key Generate.
                logging.debug(self.response)
                logging.debug(self.response['connections']['current'])
                if self.result.has_key(str(self.response['repl']['me'])) != True:
                    self.result[str(self.response['repl']['me'])] = []
                self.result[str(self.response['repl']['me'])].append(self.response['connections']['current'])
                # print "UNIQ"
                for i in self.result.keys():
                    self.result[i] = list(set(self.result[i]))
            logging.debug("LAST")
            logging.debug(self.result)
        except Exception, e:
            pass
            print "MongoDB MongodConnCount Command Error"
            print str(e)
            sys.exit(1)

    def output_result(self):
        template = "{0:10}  {1:>5}"
        for i in sorted(self.result.keys()):
            self.result[i].sort()
            for j in self.result[i]:
                print template.format(i, j)

class PrioritySearch(Command):
    """
        MasterList Command Class.
    """
    def run_command(self):
        try:
            for server in self.servers.keys():
                logging.debug(server)
                try:
                    self.response = \
                            self.servers[server].connection.local.system.replset.find()
                except Exception, e:
                    self.result['nonreplicasets'].append(server)
                    pass
                logging.debug("RES:")
                logging.debug(self.response)
                logging.debug(type(self.response))
                for restmp in self.response:
                    logging.debug(restmp['_id'])
                    if self.result.has_key(str(restmp['_id'])) != True:
                        self.result[str(restmp['_id'])] = []
                    for member in restmp['members']:
                        logging.debug(restmp['members'])
                        # when No exist priority Key , key Generate.
                        if member.has_key('priority') != True:
                            member['priority'] = 1
                        logging.debug(member['priority'])
                        if int(member['priority']) == int(self.command.priority):
                            logging.debug("OK")
                            self.result[str(restmp['_id'])].append(member['host'])
                            logging.debug(self.result[str(restmp['_id'])])
                        else:
                            logging.debug("NG")
                            pass
                # print "UNIQ"
                for i in self.result.keys():
                    self.result[i] = list(set(self.result[i]))
            logging.debug("LAST")
            logging.debug(self.result)
        except Exception, e:
            pass
            print "MongoDB PriotySearch Command Error"
            print str(e)
            sys.exit(1)

    def output_result(self):
        #template = "{1:>30}"
        #template_verbose = "{0:20}  {1:>30}"
        for i in sorted(self.result.keys()):
            self.result[i].sort()
            for j in self.result[i]:
                print j

class FsyncStop(Command):
    """
        FsyncStop Command Class.
    """
    def run_command(self):
        try:
            for server in self.servers.keys():
                print server
                self.response = self.servers[server].connection.admin.command("fsync", 1,
                        lock=1)
                #self.response = self.servers[server].connection.admin.command("fsyncLock")
                #self.response = self.servers[server].connection.admin.command("replSetGetStatus")
                logging.debug(type(self.response))
                logging.debug(self.response)
        except Exception, e:
            pass
            print "MongoDB FsyncStop Command Error"
            print str(e)
            sys.exit(1)

class FsyncStart(Command):
    """
        FsyncStart Command Class.
    """
    def run_command(self):
        try:
            for server in self.servers.keys():
                print server
                #self.response = self.servers[server].connection.admin.command("fsyncUnlock")
                #self.response = self.servers[server].connection.admin.sys.unlock.find_one()
                #self.response = self.servers[server].connection.admin.command("replSetGetStatus")
                logging.debug(type(self.response))
                logging.debug(self.response)
        except Exception, e:
            pass
            print "MongoDB FsyncStart Command Error"
            print str(e)
            sys.exit(1)

class BalancerStop(Command):
    """
        BalancerStop Command Class.
    """
    def run_command(self):
        try:
            for server in self.servers.keys():
                print server
                self.response = self.servers[server].connection.config.settings.update( { "_id":
                    "balancer" }, { "$set" : { "stopped": True } } , True )
                logging.debug(type(self.response))
                logging.debug(self.response)
        except Exception, e:
            pass
            print "MongoDB BalancerStop Command Error"
            print str(e)
            sys.exit(1)

class BalancerStart(Command):
    """
        BalancerStart Command Class.
    """
    def run_command(self):
        try:
            for server in self.servers.keys():
                print server
                #self.response = self.servers[server].connection.config.settings.find()
                self.response = self.servers[server].connection.config.settings.update( { "_id":
                    "balancer" }, { "$set" : { "stopped": False } } , True )
                logging.debug(type(self.response))
                logging.debug(self.response)
        except Exception, e:
            pass
            print "MongoDB BalancerStart Command Error"
            print str(e)
            sys.exit(1)

class BSONDump(Command):
    """
        ReplicaSetsList Command Class.
    """
    def run_command(self):
        for server in self.servers.keys():
            print server
            self.response = self.servers[server].connection.local.collection_names()
            #self.response = self.servers[server].connection.admin.command("replSetGetStatus")
            print type(self.response)
            logging.debug(self.response)
            for item in self.response:
                print "TEST"
                print item

class DelayAdd(Command):
    """
        DelayAdd Command Class.
    """
    def run_command(self):
        try:
            for server in self.servers.keys():
                self.rs_id = {}
                logging.debug(server)
                try:
                    self.response = self.servers[server].connection.local.system.replset.find()
                except Exception, e:
                    self.result['nonreplicasets'].append(server)
                    pass
                logging.debug(type(self.response))
                for restmp in self.response:
                    self.rsname = restmp['_id']
                    self.versionno = restmp['version'] + 1
                    logging.debug(self.response['set'])
                    try:
                        self.result[str(restmp['_id'])]
                    except Exception, e:
                        self.result[str(restmp['_id'])] = []
                    for member in restmp['members']:
                        try:
                            member['priority']
                        except Exception, e:
                            member['priority'] = 1
                        logging.debug(member['host'])
                        logging.debug("-------")
                        logging.debug(member)
                        self.rs_id[str(member['_id'])] = member['host']
                        if int(member['priority']) == int(0):
                            self.priority0host = member['host'].split(':')
                        else:
                            pass
                    if self.priority0host[1] == "27018":
                        self.delayport = "27020"
                    elif self.priority0host[1] == "27019":
                        self.delayport = "27021"
                    elif self.priority0host[1] == "27218":
                        self.delayport = "27020"
                    elif self.priority0host[1] == "27020":
                        self.delayport = "27020"
                    elif self.priority0host[1] == "27021":
                        self.delayport = "27021"
                    else :
                        print "bad"
                    #rs.add({_id: 3, host: "", priority: 0, slaveDelay: 21600})
                    logging.debug(self.rs_id)
                    logging.debug(self.priority0host[0] + ":" + self.delayport)
                    config = {
                        "_id": self.rsname,
                        "version": self.versionno,
                        "members": [
                        {"_id": 0, "host": self.rs_id['0'], "priority":2},
                        {"_id": 1, "host": self.rs_id['1'], "priority":1},
                        {"_id": 2, "host": self.rs_id['2'], "priority":0},
                        {"_id": 3, "host": self.priority0host[0] + ":" + self.delayport, "priority":0, "slaveDelay":10800}]}
                    print config
                    try:
                        self.response = self.servers[server].connection.admin.command("replSetReconfig",config)
                    except Exception, e:
                        print "MongoDB replSetReconfig  Error" + ": " + server
                        print str(e)
                        sys.exit(1)
                    logging.debug(self.response)
            logging.debug("LAST")
            logging.debug(self.result)
        except Exception, e:
            pass
            print "MongoDB DelayAdd Command Error"
            print str(e)
            sys.exit(1)

    def output_result(self):
        pass

class DelayAdd2(Command):
    """
        DelayAdd2 Command Class.
    """
    def run_command(self):
        try:
            for server in self.servers.keys():
                self.rs_id = {}
                logging.debug(server)
                try:
                    self.response = self.servers[server].connection.local.system.replset.find()
                except Exception, e:
                    self.result['nonreplicasets'].append(server)
                    pass
                logging.debug(type(self.response))
                for restmp in self.response:
                    self.rsname = restmp['_id']
                    self.versionno = restmp['version'] + 1
                    logging.debug(self.response['set'])
                    try:
                        self.result[str(restmp['_id'])]
                    except Exception, e:
                        self.result[str(restmp['_id'])] = []
                    for member in restmp['members']:
                        try:
                            member['priority']
                        except Exception, e:
                            member['priority'] = 1
                        logging.debug(member['host'])
                        logging.debug("-------")
                        logging.debug(member)
                        self.rs_id[str(member['_id'])] = member['host']
                        if int(member['priority']) == int(0):
                            self.priority0host = member['host'].split(':')
                        else:
                            pass
                    if self.priority0host[1] == "27018":
                        self.delayport = "27020"
                    elif self.priority0host[1] == "27019":
                        self.delayport = "27021"
                    elif self.priority0host[1] == "27218":
                        self.delayport = "27020"
                    elif self.priority0host[1] == "27020":
                        self.delayport = "27020"
                    elif self.priority0host[1] == "27021":
                        self.delayport = "27021"
                    else :
                        print "bad"
                    #rs.add({_id: 3, host: "", priority: 0, slaveDelay: 21600})
                    logging.debug(self.rs_id)
                    logging.debug(self.priority0host[0] + ":" + self.delayport)
                    config = {
                        "_id": self.rsname,
                        "version": self.versionno,
                        "members": [
                        {"_id": 0, "host": self.rs_id['0'], "priority":1},
                        {"_id": 1, "host": self.rs_id['1'], "priority":2},
                        {"_id": 2, "host": self.rs_id['2'], "priority":0},
                        {"_id": 3, "host": self.priority0host[0] + ":" + self.delayport, "priority":0, "slaveDelay":10800}]}
                    print config
                    try:
                        self.response = self.servers[server].connection.admin.command("replSetReconfig",config)
                    except Exception, e:
                        print "MongoDB replSetReconfig  Error" + ": " + server
                        print str(e)
                    #self.response = self.servers[server].connection.admin.command("replSetGetStatus")
                    logging.debug(self.response)
                #for i in self.result.keys():
                    #self.result[i] = list(set(self.result[i]))
            logging.debug("LAST")
            logging.debug(self.result)
        except Exception, e:
            pass
            print "MongoDB DelayAdd2 Command Error"
            print str(e)
            sys.exit(1)

    def output_result(self):
        pass

class DelayAdd3(Command):
    """
        DelayAdd3 Command Class.
    """
    def run_command(self):
        try:
            for server in self.servers.keys():
                self.rs_id = {}
                logging.debug(server)
                try:
                    self.response = self.servers[server].connection.local.system.replset.find()
                except Exception, e:
                    self.result['nonreplicasets'].append(server)
                    pass
                logging.debug(type(self.response))
                for restmp in self.response:
                    self.rsname = restmp['_id']
                    self.versionno = restmp['version'] + 1
                    logging.debug(self.response['set'])
                    try:
                        self.result[str(restmp['_id'])]
                    except Exception, e:
                        self.result[str(restmp['_id'])] = []
                    for member in restmp['members']:
                        try:
                            member['priority']
                        except Exception, e:
                            member['priority'] = 1
                        logging.debug(member['host'])
                        logging.debug("-------")
                        logging.debug(member)
                        self.rs_id[str(member['_id'])] = member['host']
                        if int(member['priority']) == int(0):
                            self.priority0host = member['host'].split(':')
                        else:
                            pass
                    if self.priority0host[1] == "27018":
                        self.delayport = "27020"
                    elif self.priority0host[1] == "27019":
                        self.delayport = "27021"
                    elif self.priority0host[1] == "27218":
                        self.delayport = "27020"
                    elif self.priority0host[1] == "27020":
                        self.delayport = "27020"
                    elif self.priority0host[1] == "27021":
                        self.delayport = "27021"
                    else :
                        print "bad"
                    #rs.add({_id: 3, host: "", priority: 0, slaveDelay: 21600})
                    logging.debug(self.rs_id)
                    logging.debug(self.priority0host[0] + ":" + self.delayport)
                    config = {
                        "_id": self.rsname,
                        "version": self.versionno,
                        "members": [
                        {"_id": 0, "host": self.rs_id['0'], "priority":2},
                        {"_id": 1, "host": self.rs_id['1'], "priority":1},
                        {"_id": 2, "host": self.rs_id['2'], "priority":0},
                        {"_id": 3, "host": self.priority0host[0] + ":" + self.delayport, "priority":0, "votes" : 0, "slaveDelay":10800}]}
                    print config
                    try:
                        self.response = self.servers[server].connection.admin.command("replSetReconfig",config)
                    except Exception, e:
                        print "MongoDB replSetReconfig  Error" + ": " + server
                        print str(e)
                    #self.response = self.servers[server].connection.admin.command("replSetGetStatus")
                    logging.debug(self.response)
                #for i in self.result.keys():
                    #self.result[i] = list(set(self.result[i]))
            logging.debug("LAST")
            logging.debug(self.result)
        except Exception, e:
            pass
            print "MongoDB DelayAdd3 Command Error"
            print str(e)
            sys.exit(1)

    def output_result(self):
        pass

class DelayAdd4(Command):
    """
        DelayAdd4 Command Class.
    """
    def run_command(self):
        try:
            for server in self.servers.keys():
                self.rs_id = {}
                logging.debug(server)
                try:
                    self.response = self.servers[server].connection.local.system.replset.find()
                except Exception, e:
                    self.result['nonreplicasets'].append(server)
                    pass
                logging.debug(type(self.response))
                for restmp in self.response:
                    self.rsname = restmp['_id']
                    self.versionno = restmp['version'] + 1
                    logging.debug(self.response['set'])
                    try:
                        self.result[str(restmp['_id'])]
                    except Exception, e:
                        self.result[str(restmp['_id'])] = []
                    for member in restmp['members']:
                        try:
                            member['priority']
                        except Exception, e:
                            member['priority'] = 1
                        logging.debug(member['host'])
                        logging.debug("-------")
                        logging.debug(member)
                        self.rs_id[str(member['_id'])] = member['host']
                        if int(member['priority']) == int(0):
                            self.priority0host = member['host'].split(':')
                        else:
                            pass
                    if self.priority0host[1] == "27018":
                        self.delayport = "27020"
                    elif self.priority0host[1] == "27019":
                        self.delayport = "27021"
                    elif self.priority0host[1] == "27218":
                        self.delayport = "27020"
                    elif self.priority0host[1] == "27020":
                        self.delayport = "27020"
                    elif self.priority0host[1] == "27021":
                        self.delayport = "27021"
                    else :
                        print "bad"
                    #rs.add({_id: 3, host: "", priority: 0, slaveDelay: 21600})
                    logging.debug(self.rs_id)
                    logging.debug(self.priority0host[0] + ":" + self.delayport)
                    config = {
                        "_id": self.rsname,
                        "version": self.versionno,
                        "members": [
                        {"_id": 0, "host": self.rs_id['0'], "priority":1},
                        {"_id": 1, "host": self.rs_id['1'], "priority":2},
                        {"_id": 2, "host": self.rs_id['2'], "priority":0},
                        {"_id": 3, "host": self.priority0host[0] + ":" + self.delayport, "priority":0, "votes" : 0, "slaveDelay":10800}]}
                    print config
                    try:
                        self.response = self.servers[server].connection.admin.command("replSetReconfig",config)
                    except Exception, e:
                        print "MongoDB replSetReconfig  Error" + ": " + server
                        print str(e)
                    logging.debug(self.response)
            logging.debug("LAST")
            logging.debug(self.result)
        except Exception, e:
            pass
            print "MongoDB DelayAdd4 Command Error"
            print str(e)
            sys.exit(1)

    def output_result(self):
        pass

class ShardCollectionList(Command):
    """
        ShardCollectionList Command Class.
    """
    def run_command(self):
        try:
            for server in self.servers.keys():
                logging.debug(server)
                try:
                    self.response = \
                    self.servers[server].connection.config.collections.find()
                except Exception, e:
                    self.result['nonreplicasets'].append(server)
                    pass
                logging.debug(type(self.response))
                for restmp in self.response:
                    logging.debug(type(self.result))
                    # when No exist Key , key Generate.
                    logging.debug("Dropped Flag: ")
                    logging.debug(restmp["dropped"])
                    if self.result.has_key(server) != True:
                        self.result[server] = []
                    if restmp["dropped"] == True:
                        continue
                    self.result[server].append(restmp["_id"])
                for i in self.result.keys():
                    self.result[i] = list(set(self.result[i]))
            logging.debug("LAST")
            logging.debug(self.result)
        except Exception, e:
            pass
            print "MongoDB ShardCollectionList Command Error"
            print str(e)
            sys.exit(1)

    def output_result(self):
        #template = "{1:>30}"
        #template_verbose = "{0:20}  {1:>30}"
        for i in sorted(self.result.keys()):
            self.result[i].sort()
            for j in self.result[i]:
                print j

class CurrentOpList(Command):
    """
        CurrentOpList Command Class.
    """
    def run_command(self):
        try:
            for server in self.servers.keys():
                # DebugLog
                logging.debug(server)
                try:
                    self.response = \
                    self.servers[server].connection.admin.current_op()
                except Exception, e:
                    self.result['nonreplicasets'].append(server)
                    pass
                #logging.debug(type(self.response))
                logging.debug(self.response['inprog'])
                for i in self.response['inprog']:
                    logging.debug('DATA:' + i)
                    # init self.result[] or dict append.
                    # when No exist Key , key Generate.
                    if self.result.has_key(str(self.response['set'])) != True:
                        self.result[str(self.response['set'])] = []
                    if not i['name'] in self.result[str(self.response['set'])]:
                        self.result[str(self.response['set'])].append(i['name'])
                        logging.debug(self.response['set'])
                    logging.debug(self.result[str(self.response['set'])])
                # print "UNIQ"
                for i in self.result.keys():
                    self.result[i] = list(set(self.result[i]))
            logging.debug("LAST")
            logging.debug(self.result)
        except Exception, e:
            pass
            print "MongoDB CurrentOpList Command Error"
            print str(e)
            sys.exit(1)

    def output_result(self):
        template = "{0:20}  {1:>30}"
        tmpi =  ""
        for i in sorted(self.result.keys()):
            self.result[i].sort()
            for j in self.result[i]:
                if  tmpi == i:
                    print template.format("", j)
                else:
                    print template.format(i, j)
                    tmpi = i

class TEST(Command):
    """
        ReplicaSetsList Command Class.
    """
    def run_command(self):
        for server in self.servers.keys():
            print "TEST COMMAND EXEC: OK"
            print server
            #self.response = self.servers[server].connection.local.system.replset.find_one()
            #self.response = self.servers[server].connection.admin.command("replSetGetStatus")
            #self.response = self.servers[server].connection.config.collections.find_one()
            #self.response = self.servers[server].connection.config.collections.find()
            #self.response = self.servers[server].connection.admin.command("serverStatus")
            self.response = \
                    self.servers[server].connection.admin.current_op()
            #self.servers[server].connection.admin.command("listShards")
            #self.servers[server].connection.admin.command("getShardVersion")
            #logging.debug(type(self.response))
            #logging.debug(self.response['repl']['me'])
            #logging.debug(self.response)
            #logging.debug(self.response['_id'])
            #logging.debug(self.response)
            #for item in self.response:
            #for item in sorted(self.response.keys()):

            print self.response
            for item in self.response:
                print item
                logging.debug("TEST")
            #    print item['_id']


if __name__ == '__main__':
    pass


