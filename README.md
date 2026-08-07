# distributed-mq

A distributed message queue built from scratch in Java — an append-only, partitioned, replicated
log broker in the spirit of Kafka. It is not a Kafka clone: it is a subset large enough to actually
exercise the hard parts (segmented log storage, sparse indexing, crash recovery, replication, ISR,
consumer groups) without the twenty years of production surface area.

The full technical spec lives in [.claude/PROJECT.MD](.claude/PROJECT.MD).

## Status

Phase 1 (log storage engine) is done and Phase 2 (broker networking) is in progress.

| Phase | Scope | Status |
|---|---|---|
| 1 | Segmented log, offset/time indexes, recovery, `dump-log` | done |
| 2 | Binary protocol, Netty broker, produce/fetch, minimal clients | protocol done, server pending |
| 3 | Topics, partitions, metadata, producer batching | not started |
| 4 | Consumer groups, coordinator, rebalance | not started |
| 5 | Replication, ISR, high watermark, `acks=all` | not started |
| 6 | Controller (Raft-lite), leader failover | not started |
| 7 | Retention, compaction, idempotent producer | not started |
| 8 | Metrics, benchmarks, docs | not started |

## What works today

**Log storage engine** (`mq-broker`)

- Append-only segments named after their base offset (`00000000000000016384.log`), rolled by
  `segment.bytes` or `roll.ms`.
- Sparse `.index` (8-byte entries) and `.timeindex` (12-byte entries) mapped through the FFM API,
  so buffers are unmapped deterministically instead of waiting on the GC.
- Reads resolve a segment via a `NavigableMap` floor lookup, jump into the log through the index,
  and never return a partial batch — if the first batch alone exceeds `maxBytes` it comes back
  whole, otherwise a consumer with a small fetch size would stall forever.
- `offsetForTimestamp` walks the time index, then scans forward for the first record at or after
  the target timestamp.
- Crash recovery: a clean shutdown leaves a marker plus a recovery-point checkpoint. Without the
  marker, segments are re-scanned, the corrupt or torn tail is truncated at the last intact batch,
  and both indexes are rebuilt. Deleting an index file by hand is a supported repair path.

**Record format** (`mq-common`)

Big-endian `RecordBatch` with a 61-byte header, CRC32C over everything after the checksum field,
and fixed-width records. Kafka uses varints here; fixed width is far easier to debug with a hex
editor and is noted as a future optimization.

**Protocol** (`mq-common`)

Length-prefixed framing over TCP (`size | apiKey | apiVersion | correlationId | clientId | body`),
Kafka-aligned api keys and error codes, and encoders/decoders for Produce, Fetch and ApiVersions.
Frames above 100 MiB or with a negative size kill the connection instead of allocating.

**CLI** (`mq-cli`)

`dump-log` prints batch headers, record payloads and index sanity checks — the tool you reach for
first when a log looks wrong.

## Stack

Java 25, Gradle 9.5 (multi-module), Netty 4.1, SLF4J/Logback, JUnit 5 + AssertJ.
`mq-broker` and `mq-client` have no Spring dependency; Spring Boot is confined to `mq-admin`.

## Modules

```
mq-common   protocol, codecs, record format, framing
mq-broker   log storage engine, cluster and networking internals
mq-client   producer and consumer (in progress)
mq-cli      admin and debugging tools
mq-it       integration and chaos tests
mq-admin    optional Spring Boot admin surface
```

## Running

```bash
./gradlew build          # compile everything and run the test suite
./gradlew test           # tests only
```

Inspect a segment file:

```bash
./gradlew :mq-cli:installDist
./mq-cli/build/install/mq-cli/bin/mq-cli dump-log \
    --files data/orders-0/00000000000000000000.log --print-data-log
```

For local development set `log.segment.bytes=16384` — with the 1 GiB default you never see a
segment roll and half the storage logic goes untested.
