# astacus

Astacus is a clustered database backup system that is meant to work with
multiple open-source cluster databases, such as
[M3](https://github.com/m3db/m3/) and
[Apache Cassandra](https://cassandra.apache.org).

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

- Support list of object storage backup site locations -> Facilitate
  migration from old to new during service cloud migration

- Have most of the code covered by unit tests

- Can be open sourced without feeling too bad about it
    - Open source from the get go, no proprietary legacy dependencies

# See also

- [Design overview](doc/design/overview.md)
- [Implementation overview](doc/design/implementation.md)

# Installation

Please see Dockerfile.fedora and Dockerfile.ubuntu for concrete up-to-date
examples, but here are the current ones:

## Optional features

- cassandra can be added with 'cassandra' optional:
```
sudo pip3 install -e '.[cassandra]'
```



## Fedora 37

(as root or user with sudo access; for root, skip sudo prefix)

```
sudo dnf install -y make
make build-dep-fedora
sudo python3 ./setup.py install
```

## Ubuntu 20.04

(as root or user with sudo access; for root, skip sudo prefix)

```
sudo apt-get update
sudo apt-get install -y make sudo
make build-dep-ubuntu
sudo python3 ./setup.py install
```


# Configuration

Create astacus.conf, which specifies which database to back up, and where.
The configuration file format is YAML, but as it is JSON superset, JSON is
also fine.

Unfortunately the configuration part is not particularly well documented at
this time, but there are some examples of file backups to
[local directory (JSON)](examples/astacus-files-local.json),
[local directory (YAML)](examples/astacus-files-local.yaml), [Amazon S3](examples/astacus-files-s3.json), or
[Google GCS](examples/astacus-files-gcs.json). There is even one example of
[backing up M3 to GCS](examples/astacus-m3-gcs.json).


# Usage

## Start the nodes

Start astacus server on all nodes to be backed up, either by hand or via
e.g. systemd:

`astacus server -c <path to configuration file>`

## Perform backups

Periodically (e.g. from cron) call on (ideally only one node, but it
doesn't really matter as only one operation can run at a time):

- `astacus backup` or
- HTTP POST to http://server-address:5515/backup


## Restore backups

Backup can be restored with either

- `astacus restore` or
- HTTP POST to http://server-address:5515/restore

## List backups

To see list of backups:

- `astacus list` or
- HTTP GET http://server-address:5515/list

## Clean up old backups

To clean up backups based on the configured retention policy,

- use `astacus cleanup` (from cronjob or CLI), or
- HTTP POST to http://server-address:5515/cleanup


# TODO

There is separate [TODO](TODO.md) file which tracks what is still to be done.
