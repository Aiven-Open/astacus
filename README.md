# astacus

Astacus is a clustered database backup system that is meant to work with
multiple open-source cluster databases, such as [M3][m3] and and [Apache Cassandra][cassandra].

_My name is Maximus Backupus Astacus, Co-ordinator of the Backups of the
Cluster, Master  of the Storage Availability, loyal servant to the true
emperor, Prunus Aivenius. Father to a failed backup, husband to a corrupted
data. And I will have my restore, in this runtime or the next._

# Goals

- Support multiple clustered database products
- Most of the code generic
- Product-specific code with simple, testable API

- Complexities to deal with e.g. reuse of blobs with same value in the
shared code
    - It is needed to accomplish e.g. fast non-incremental M3 backups that are
    essentially incremental as only commit logs change frequently

- Support list of object storage backup site locations -> Facilitate migration
  from old to new during service cloud migration

- Have most of the code covered by unit tests

- Can be open sourced without feeling too bad about it
    - Open source from the get go, no proprietary legacy dependencies

# Installation

Use setup.py, or install from pip (TBD)

# Configuration

Create astacus.conf, which specifies which database to back up, and where.

# Usage

## Start the nodes

Start astacus server on all nodes to be backed up:

`astacus server -c <configuration path>`

## Perform backups

Periodically (e.g. from cron) call on (ideally only one node, but it
doesn't really matter):

- `astacus backup` or
- HTTP POST to http://server-address:5515/backup


## Restore backups

Backup can be restored with either

- `astacus restore` or
- HTTP POST to http://server-address:5515/restore

## List backups

To see list of backups:

- `astacus list` or
- HTTP GET http://server-address:5515/list)

## Clean up old backups

To clean up backups based on the configured retention policy,

- use `astacus cleanup` (from cronjob or CLI), or
- HTTP POST to http://server-address:5515/cleanup


# TODO

There is separate [TODO](TODO.md) file which tracks what is still to be done.

[cassandra]: https://cassandra.apache.org
[m3]: https://github.com/m3db/m3/
