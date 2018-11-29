# Specious: a simple, persistent, key-value store

A simple key-value store following the design of LevelDB, as a prototype of a verified key-value store.

We are aiming for the following basic design:
- crash safety using a write-ahead log
- transactional writes (which use the same transaction)
- recovery
- background compaction
