# Overview of Astacus design

While more thorough documentation would be welcome, time constraints are
what they are, so here's very brief overview of the Astacus design.

## Components

Astacus is designed so that every cluster node runs it. Internally, it has
two parts:

- `astacus.coordinator`, which is responsible for using set of Astacus
  'node' components to perform user-requested operation. The REST endpoints
  it provides are mostly asynchronous.

- `astacus.node`, which is responsible for performing (parts) of tasks
  required by users that apply to the current operation being performed. A
  lot of it is implemented as synchronus REST endpoints to be used
  internally by the coordinator.

Additionally, the project also contains `astacus.common` which contains
code shared by `astacus.coordinator` and `astacus.node`.

## Concurrency handling

Astacus does NOT have concept such as 'master node'; instead, when
requested by someone (user, another program) to perform something using
REST API of the coordinator component, Astacus will acquire lock on the
cluster's 'node' components, and only them perform the operation. If this
is not possible, nothing is done and error is returned.

The locks are timed, and have to be refreshed regularly. If this fails, the
associated operations will also fail.

## Plugins

Astacus has concept of 'plugins' in the 'coordinator' part; it allows
adding extra functionality within to handle the software-specific
needs. For example, in case of M3, this handles backing up and restoration
of subset of etcd state.

## Operations of the coordinator

The set of steps depends somewhat by plugin, but here is the general
description of what each of the operations coordinator support do under the
hood.

### Backup (locks cluster for exclusive use)

Backup takes 'snapshot' of the file hierarchy it is associated with; it
makes hardlinks for the files, and remembers size of the files at the time
of the snapshot. The assumption is that if the files change, they are only
appended to; mutating them in place is not supported.

Once snapshot is taken, it is stored to the chosen storage target (files,
cloud storage), as well as the manifest files. The files are uploaded by
hash, and therefore if same files exist in multiple locations on one or
multiple nodes, they will consume space only once in the backup
location.

Additionally, plugin-specific steps may be taken (e.g. in case of M3, etcd
subset backup).

Finally, backup manifest is written to the backup location as well
which describes which files were on which nodes (and plugin-specific extra
information).

### Restore (locks cluster for exclusive use)

Restore loads backup manifest from the backup location, and then tells each
node to restore its own subset of files.

Additionally, plugin-specific steps may be taken (e.g. in case of M3, etcd
subset restore).

### Cleanup old backups (locks cluster for exclusive use)

Astacus will clean up the backup location based on retention criteria, and
delete all object hashes to which there is no reference anymore in the
retained backups.

While this operation technically does not do anything on the other nodes,
it is necessary to retain the cluster lock to prevent e.g. freshly uploaded
blocks from being deleted.

### List backups

This is (so far) the only operation which does not take cluster lock; it
will fetch list of backup manifests, caching their content locally so that
subsequent returns or use of their content is rapid.
