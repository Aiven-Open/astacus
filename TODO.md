# List of things that need to be implemented #


## Short-term

- document
    - (better) README
    - the design
    - user-visible UI etc

- measure, improve test code coverage

- more metrics endpoints - think on what is really needed
    - perhaps backup/snapshot/restore sizes and file counts? copy from *hoard?


## Eventually

- Cassandra partial restore support; notably, we should be able to recover
  single nodes (perhaps in this case, if version matches, restore both
  system keyspace AND user keyspaces, and avoid the create steps in full
  restore)

- Cassandra specific table restore support; we should then pick more
  carefully where exactly we are restoring things:
  - get table ids for freshly created tables (during restore when Cassandra
    is up from system_schema.tables)
  - pass the keyspace+table+table id tuples to the actual restore Cassandra
    step
    - clear the directory
    - move files to it


- package (or have someone do it?) this for distros

- page the result from operations (as it is, some of the stuff may take
  e.g. 30 seconds to get across in our prod cluster)

- push this to PIP

- use result-url instead of polling for somewhat faster results for CLI

- we should have some sort of packfile format for small files - right now,
  the embed-in-manifest kind of works, but is not pretty, and doesn't work
  for still trivial sized files


## Maybe not - known design choice for now

- in m3db placement plan, we do not rewrite port number

- it would be possible to split (large) BackupManifest to
  e.g. BackupSummary which would be used by list backups endpoint; in
  practise, the results should be cached locally so cost of downloading
  (even largish) files once is not prohibitive

- permissions / file modes and directories/links in general are ignored. if
  this is actually used for general cluster backup, actually storing those
  would be nice too and not too much effort. however, it is not really
  focus of the project for now so left not done.
