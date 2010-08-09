# CouchDBCP

## Abstract
[CouchDBCP](http://github.com/KlausTrainer/couchdbcp) is an acronym for **CouchDB** **C**lustering **P**roxy. Basically, it's a *reverse* proxy (a.k.a. gateway) for maintaining [CouchDB clusters](http://mambofulani.couchone.com/blog/_design/sofa/images/couchdb_cluster.png). Its objective is to allow for an abstraction of a single reliable CouchDB device, using a collection of possibly unreliable CouchDB units.

## Motivation
The initial motivation for CouchDBCP was to have a solution for managing CouchDB nodes in a decentralized way, so that one can have control over the consistency level and replication behavior. Imagine you have an application scenario that requires atomic data consistency in favor of availability just for a small subset of your data. Wouldn't it be cool, being able to control the consistency level on a per-request basis?

## Description
As of now, CouchDBCP does not support data partitioning. It can only be used to manage redundant CouchDB instances in a decentralized way. As long as data partitioning is not available, it is intended that each CouchDBCP is assigned only one CouchDB node. A CouchDBCP is used as a gateway (more precisely as a reverse HTTP proxy) in front of a CouchDB. In order to minimize latency, the two components are intended to be located nearby, connected with a high speed, low-latency network link; if not just simply located on the same computer.

Currently, most Futon tests are passing. Some are failing because the REST constraint of stateless communication is violated. Furthermore, there are a few tests that sometimes fail due to timeouts when the CouchDB instances are frequently restarted by the test. There should be room for improvement regarding socket communication, timeouts and error handling.

Regarding cookie authentication, CouchDBCP is currently able to maintain authentication state: If atomic consistency is used for cookie authentication (i.e., the `POST` request to `/_session`), it is possible to load-balance requests over all cluster nodes, without losing authentication.

I don't know, however, whether OAuth will ever work cluster-wide, since OAuth is significantly more complex than cookie authentication.

For read requests, both eventual and atomic consistency are supported. However for write requests, only atomic consistency is currently implemented. In practice that means that a cluster won't be available for a write operation when there is no quorum of nodes being able to commit.

## Future Goals
Future goals are:
* allowing eventual consistency for write operations (will soon be available)
* decentralized cluster configuration using an HTTP-based gossip protocol
* data partitioning support based on consistent hashing (c.f. Riak)
* cluster monitoring


# Version
Version 0.1 - 2010-08-02


# Version History
Version 0.1
    - Initial version.


# Dependencies
CouchDBCP depends on [MochiWeb](http://github.com/mochi/mochiweb) and [ibrowse](http://github.com/cmullaparthi/ibrowse). Make sure that these libraries are available on your system.

# Installation

## Compilation
To compile CouchDBCP, simply run `make`. At this point in time, there is no `install` target. For compilation, [MochiWeb](http://github.com/mochi/mochiweb) and [ibrowse](http://github.com/cmullaparthi/ibrowse) (see Dependencies) are expected to be located in CouchDBCP's parent directory. If the libraries are located elsewhere, you need to set the symbolic links in `deps` accordingly.

## Configuration
CouchDBCP's configuration is done in `config/couchdbcp.erlenv`. Logging is configured in `config/elog.config`.

See the included sample configuration.


# Usage

## Launching CouchDBCP
There are two scripts included to launch CouchDBCP: one for development (`start-dev.sh`), and one for a production environment (`start.sh`). To launch CouchDBCP, the first argument you need to specify is a yet unused node name (that is specified in the configuration file `config/couchdbcp.erlenv`). The second argument is the configuration file itself, e.g.:

    ./start-dev.sh proxy-1@127.0.0.11 config/couchdbcp.erlenv

Of course, you also need to make sure that the CouchDB instance assigned to the respective CouchDBCP is up.

## Consistency Semantics
CouchDBCP supports two different data consistency levels to be set in the configuration file (`config/couchdbcp.erlenv`). However, the configured consistency level can be overridden on a per-request basis, simply by setting one of the following custom HTTP request headers:
* `X-CouchDBCP-Consistency: atomic`
* `X-CouchDBCP-Consistency: eventual`


# Contributors
* Klaus Trainer - original author
* You! Come on...
