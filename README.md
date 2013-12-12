mongodb-Tools
=============

MongoDB Admin Tools

# Examples

## replicasets list
    $ ./mongodb-rslist.py -f testmongod.list
    ReplicaSets001                  192.168.0.1:27218
                                    192.168.0.2:27218
                                    192.168.0.3:27218
                                    192.168.0.3:27518
    ReplicaSets002                  192.168.0.4:27218
                                    192.168.0.5:27218
                                    192.168.0.6:27218
                                    192.168.0.6:27518

## search priority==2 member
    $ ./mongodb-rssearch.py -f testmongod.list -p 2
    192.168.0.1:27218
    192.168.0.4:27218

## shard collection list
    $ ./mongodb-shardcol.py -f testmongos.list
    db.collection001
    db.collection002
    db.collection003
    db.collection004

## replcasets master node list
    $ ./mongodb-masterlist.py -f testmongod.list
    192.168.0.1:27218
    192.168.0.4:27218

## mongod connection count list
    $ ./mongodb-conncount.py -f testmongod.list
    192.168.0.1:27218     23
    192.168.0.2:27218     13
    192.168.0.3:27218     13
    192.168.0.4:27218     23
    192.168.0.5:27218     14
    192.168.0.6:27218     14

## mongos balancer stop
    $ ./mongodb-balancerstop.py -f testmongos.list
    192.168.0.9:27017

## mongos balancer start
    $ ./mongodb-balancerstart.py -f testmongos.list
    1192.168.0.9:27017


# Documentation
Not Yet...


# How to contribute

## Report a bug

https://github.com/akuwano/mongodb-Tools/issues

# For developers
## install modules

    $ pip install pymongo==2.6.3
 

