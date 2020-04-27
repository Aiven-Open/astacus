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

- List of object storage “backup site” locations -> Facilitate migration
  from old to new during service cloud migration

- Have most of the code covered by unit tests

- Can be open sourced without feeling too bad about it
    - Open source from the get go, no proprietary legacy dependencies

# Installation

TBD

# Usage

TBD

[cassandra]: https://cassandra.apache.org
[m3]: https://github.com/m3db/m3/
