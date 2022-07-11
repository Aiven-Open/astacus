# Astacus for ClickHouse

- Requires [ClickHouse >= 21.11](https://clickhouse.com/docs/en/whats-new/changelog/#clickhouse-release-v21-11-2021-11-09)
- Backup of databases using
  the [Replicated database engine](https://clickhouse.tech/docs/en/engines/database-engines/replicated/)
- Backup of content of tables using
  the [MergeTree table engines](https://clickhouse.tech/docs/en/engines/table-engines/mergetree-family/), including the
  Replicated variants.
- Backup of schema of all other table engines
- Backup of users using
  the [`replicated` storage](https://clickhouse.tech/docs/en/operations/server-configuration-parameters/settings/#user_directories)
  on ZooKeeper

## Astacus Configuration

```json
{
  "zookeeper": {
    "nodes": [
      {
        "host": "zk1.example.org",
        "port": 5555
      }
    ]
  },
  "clickhouse": {
    "username": "user",
    "password": "password",
    "nodes": [
      {
        "host": "ch1.example.org",
        "port": 9000
      },
      {
        "host": "ch2.example.org",
        "port": 9000
      },
      {
        "host": "ch3.example.org",
        "port": 9000
      }
    ]
  },
  "replicated_access_zookeeper_path": "/clickhouse/access",
  "replicated_databases_zookeeper_path": "/clickhouse/databases",
  "replicated_databases_settings": {
    "max_broken_tables_ratio": 0.5,
    "max_replication_lag_to_enqueue": 10,
    "wait_entry_commited_timeout_sec": 3600,
    "cluster_username": "distributed_user",
    "cluster_password": "distributed_user_password",
    "cluster_secret": "the secret"
  },
  "freeze_name": "astacus",
  "sync_timeout": 3600
}
```

## ClickHouse Configuration

### Enabling replicated users

This should be part of your main ClickHouse configuration file :

```xml

<clickhouse>
    <user_directories>
        <users_xml>
            <path>users.xml</path>
        </users_xml>
        <replicated>
            <zookeeper_path>/clickhouse/access/</zookeeper_path>
        </replicated>
    </user_directories>
</clickhouse>
```

The `<users_xml>` section before the `<replicated>` section is useful to configure your admin user, which can be then used to
create other users, which will be replicated.

### Enabling support for the Replicated database engine

First make sure your ClickHouse version includes [this patch](https://github.com/ClickHouse/ClickHouse/pull/29202).

Then enable the Replicated database engine in your `users.xml` file:

```xml

<clickhouse>
    <profiles>
        <default>
            <allow_experimental_database_replicated>true</allow_experimental_database_replicated>
        </default>
    </profiles>
</clickhouse>
```

You can now create databases on all servers of the cluster using:

```sql
CREATE DATABASE ` db_name ` ENGINE = Replicated('/clickhouse/databases/db_name', '{shard}', '{replica}')
```

## Restoration behavior

The synchronization step exchanges replicated data between all servers of the cluster.
If you have large volumes of data, adjust the `sync_tables_timeout` (in seconds) accordingly.

### Databases

Replicated database are restored with their original name. However, they are not restored with the same UUID.

The replicated databases will be restored with the settings from the configuration files, not their original settings.

Their restored ZooKeeper path is constructed from `replicated_databases_zookeeper_path` and the escaped name of the
database (escaped by replaced all characters which are not alphanumerical or `_` with their percent-encoded equivalent):

The database named `thing_3a-beta` will be created with a ZooKeeper path of
`/clickhouse/databases/thing_3a%2Dbeta`.

### Tables

Tables are restored with their original name, UUID and settings.

### Access Entities: Users, Quotas, Row Policies, Setting Profiles, Grants

All these access entities are restored with their original name, UUID and all the details attached to them.
