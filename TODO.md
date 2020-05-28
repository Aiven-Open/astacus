# List of things that need to be implemented #

Note: This is more granular and detailed version what should be done; there
is also (Aiven-internal) backlog of Astacus tickets that track subset of these.

## Very short term; usable for internal use

- backup cleanup endpoint + its tests

- backup list endpoint + its tests

- improve operation reporting; probably astacus.common.progress.Progress
  information should be also forwarded to coordinator results, and
  subsequently to e.g. CLI. Currently coordinator REST API / CLI reports
  just binary outcome (success/not) which while technically sufficient in
  short term, isn't optimal

- plugin concept in general - how do extra parts in backup/restore work?
     - m3 plugin
          - one node will ensure that etcd state has stayed consistent across
          snapshot+upload period, and it will be stored also within backup
          - at restoration time, one node will be used to restore etcd
          contents, with rewritten node names

- selective caching layer for astacus.common.storage.Storage; basically, we
  want to avoid *unneccessary* object storage access, and as all objects we
  deal with are immutable, it should be pretty straightforward to implement
  and use

## Short-term; before public availability

- multiple storages actually in use (/tested to work); rohmustorage has
  base of the code but all other code should also use similar logic. this
  is so that if e.g. service is migrated from storage x to y, new backups
  will use storage location y, but old backups at storage x are also
  available (and transparently to the user)

- document
    - (better) README
    - the design
    - user-visible UI etc

- cassandra plugin

- measure, improve test code coverage

## Eventually

- package (or have someone do it?) this for distros

- separate rohmu from pghoard (right now, pghoard dependency is bit .. ugly ..)
