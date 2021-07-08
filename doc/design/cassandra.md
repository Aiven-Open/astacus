# Cassandra backup/restore mechanics #

There is a large number of existing projects that implement Cassandra
backups, but none of them fit quite the criteria we wanted:

- reasonably stand-alone and automatable
- data-at-reste security built in
- compression
- deduplication

and due to that the Cassandra support implementation for Astacus was
started.

## Cassandra backup format

Astacus backups consist of generic manifest, and then per-node snapshot
results, as well as plugin specific metadata.

We store Cassandra schema in the plugin specific metadata, and take
Cassandra snapshot of all nodes being backed up and back that up using the
generic Astacus mechanism.

References in the code:
- astacus/common/ipc.py (Generic backup manifest)
- astacus/coordinator/plugins/cassandra/ (Cassandra plugin specific parts)

## Backup process

See the Cassandra plugin for details, but basically we do following steps
(and as per Astacus philosophy, steps are either retried or whole operation
fails if something unexpected occurs):

1. validate configuration seems sane and matches cluster

1. ensure hash of schema of all nodes is consistent

1. retrieve actual schema from current node

1. remove old Cassandra snapshot (if any)

1. take Cassandra snapshot

1. retrieve hash of the schema from all nodes and ensure it matches hash
   from the first step

1. (common Astacus code) take Astacus snapshot of Cassandra snapshot (=
   calculate hashes of all files in Cassandra snapshot directories that
   have changed since previous iteration)

1. (common Astacus code) check object storage for which hashes already
   exist there

1. (common Astacus code) upload the objects that are missing from the
   object storage to the cloud

1. upload backup manifest

## Restore process

1. validate configuration seems sane and matches cluster

1. retrieve backup manifest

1. parse plugin-specific part of the manifest

1. start Cassandra on the nodes, with auto bootstrap disabled, and initial
   tokens based on backup manifest (= call configured start script with the
   edited configuration file)

1. wait until Cassandra nodes are up

1. (ensure hash of schema of all nodes is consistent - TBD, we don't do
   this at the moment, is it worth doing?)

1. pre-data-restore restore schema to the cluster (e.g. normal tables)

1. power off Cassandras

1. (common Astacus code) restore the snapshotted data to the nodes

1. move the tables' data from snapshot directories to the actual tables'
   directories

1. start Cassandra on the nodes again

1. wait until Cassandra nodes are up

1. post-data-restore restore schema to the cluster (e.g. indexes)
