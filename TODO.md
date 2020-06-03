# List of things that need to be implemented #

Note: This is more granular and detailed version what should be done; there
is also (Aiven-internal) backlog of Astacus tickets that track subset of these.


## Very short term; need to be done for it to be usable for internal use

- backup cleanup endpoint + CLI + its tests
    - maintain only up to N backups

- backup delete endpoint + CLI + its tests
    - specific backups' manual deletion

- backup list CLI + its tests
    - one option for creating nice looking output for it would be https://pypi.org/project/tabulate/

- improve operation reporting; probably astacus.common.progress.Progress
  information should be also forwarded to coordinator results, and
  subsequently to REST/CLI. Currently coordinator REST API / CLI reports
  just binary outcome (success/not) which while technically sufficient in
  short term, isn't optimal

- more metrics endpoints - think on what is really needed
    - perhaps backup/snapshot/restore sizes and file counts? copy from *hoard?

- plugin
    - (partial?) cassandra plugin; mostly to validate plugin arch is broad enough

- sync package dependencies ( setup.cfg, requirements*.txt mainly ) with
  what is used internally ; so that astacus.spec winds up with same
  versions, given Aiven-internal packages)


## Short-term; before public availability

- document
    - (better) README
    - the design
    - user-visible UI etc

- measure, improve test code coverage


## Eventually

- m3aggregator plugin (m3db is priority)

- package (or have someone do it?) this for distros

- separate rohmu from pghoard (right now, pghoard dependency is bit .. ugly ..)


## Maybe not - known design choice for now

- in m3db placement plan, we do not rewrite port number

- it would be possible to split (large) BackupManifest to
  e.g. BackupSummary which would be used by list backups endpoint; in
  practise, the results should be cached locally so cost of downloading
  (even largish) files once is not prohibitive
