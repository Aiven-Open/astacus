# List of things that need to be implemented #

Note: This is more granular and detailed version what should be done; there
is also (Aiven-internal) backlog of Astacus tickets that track subset of these.


## Short-term; before public availability

- document
    - (better) README
    - the design
    - user-visible UI etc

- improve operation reporting; probably astacus.common.progress.Progress
  information should be also forwarded to coordinator results, and
  subsequently to REST/CLI. Currently coordinator REST API / CLI reports
  just binary outcome (success/not) which while technically sufficient in
  short term, isn't optimal

- measure, improve test code coverage

- more metrics endpoints - think on what is really needed
    - perhaps backup/snapshot/restore sizes and file counts? copy from *hoard?

- plugin
    - (partial?) cassandra plugin; mostly to validate plugin arch is broad enough


## Eventually

- figure out why S3 didn't work - I got
  `botocore.exceptions.ClientError: An error occurred (AccessDenied) when
  calling the ListObjectsV2 operation: Access Denied`
  with allegedly properly configured token+bucket

- package (or have someone do it?) this for distros

- separate rohmu from pghoard (right now, pghoard dependency is bit .. ugly ..)

- use result-url instead of polling for somewhat faster results for CLI


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
