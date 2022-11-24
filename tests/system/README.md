# System tests for Astacus

_NOTE_: No tests are planned for integration with e.g. etcd or m3 directly,
at least outside container, as both requirements for installing them as
well as starting and running them takes considerable amount of time. These
system tests are only for pieces within Astacus, to ensure that the core
pieces work properly together.

## Overall idea

- start N Astacus processes with shared (file-based) object storage (but distinct root)

- back up from one

- restore to another

- ensure result is same
