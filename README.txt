Summary
=======

This is a proxy to be placed between HandlerSocket servers and Memcached clients. It translates between MC and HS protocols. It's based on libevent (async IO).

It has been tested in production environments reaching about 250K with 8 cores (per proxy). Using HS and HSProxy it's possible to reach up to 1 million random reads per second and per MySQL server (from data stored in memory).

It supports ASCII and Binary protocols for MC requests (all get commands). We don't use HS for write operations or range reads, we prefer to use SQL interface for these operations since we have no benefit of using HS).

MC keys depends on table name, partition (if using sharding), key version and primary key: e.g. user_basic:12@3:65123

Each MC key version is related with a different field list.

Requisites
==========
- libevent2-dev
- libdaemon-dev
- yaml-cpp 0.5.1 (http://code.google.com/p/yaml-cpp/)
- libmemcached-dev
To compile: make

Using it
========
- You need a MySQL with HS plugin enabled
- HSProxy will read config files (in specified config path)
- Using data in config files HSProxy will connect with HS server and open indexes
- MC clients can start sending MC Get requests to HSProxy that will convert into HS read requests
To run it: hsproxy -c <config_path>

Contents
========
- server: this folder contains all hsproxy code
- config: some sample config files
- client: some clients used for debugging, profiling...
