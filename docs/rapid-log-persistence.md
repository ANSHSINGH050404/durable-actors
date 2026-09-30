# Rapid append-log persistence

Rapid persistence uses append logs in exactly two distinct zones, with Standard GCS for durable manifests, checkpoints, and archived history. Configure the two buckets and archive through `DURABLE_ACTORS_RAPID_BUCKETS` and `DURABLE_ACTORS_ARCHIVE_BUCKET`, or Helm `storage.rapid.buckets` and `storage.archiveBucket`.

This is a breaking development format change. Use fresh ownership records or explicitly import state before replacing an earlier deployment.

## Write and ownership protocol

The host begins opening a unique appendable object in each zone as soon as it receives its actor identity, concurrently with code installation and state loading. After acquiring ownership with a generation-conditional update, it publishes an immutable segment manifest to Standard storage in the background. Read-only activation does not wait for publication. The manifest binds both exact object generations to the actor's ownership epoch and state stream. No state write is acknowledged before that manifest is durable.

Each write appends the same framed, complete state snapshot to both open streams. The frame contains a marker, length, state version, and SHA-256 checksum. The stored snapshot also carries its ownership epoch, request ID, result, and attribution. Both flushes must report the exact expected persisted offset. There is no per-write finalize, rename, owner update, or Standard upload.

The existing actor persistence wrapper checks lease authority before and after the write. A failed, timed-out, or cancelled append poisons the session and terminates its activation. It cannot continue on a potentially incomplete stream. If a new activation cannot open both Rapid streams, it uses immutable Standard snapshots for that activation.

## Checkpoints and recovery

Segments rotate at 8 MiB. A worker checks every 60 seconds and checkpoints active segments when at least 60 seconds have elapsed since the preceding checkpoint; the first checkpoint can therefore take nearly two intervals. Shutdown archives the segment and writes the latest snapshot to Standard before marking ownership sealed. Rapid copies are deleted only after the complete segment has been archived successfully. State snapshots larger than 4 MiB use Standard, preserving the existing state-size behavior.

A clean resume loads the snapshot referenced by sealed ownership. An unclean resume requires the old lease to expire, validates the durable manifests, and reopens the available object generations to fence their old streams. It verifies complete records and rejects corruption, incompatible replicas, and mismatched actor/epoch identities. Incomplete trailing bytes are ignored.

Because every acknowledged Rapid record reached both zones, one available replica suffices to recover it. A complete uncertain tail may also be recovered; the selected state is written to Standard before the ownership epoch advances. The request outcome can therefore be uncertain after an interrupted write, as with any interrupted persistence operation. Recovery never silently substitutes an older checkpoint when both replicas of an unarchived segment are unavailable.

State inspection and history expose the original logical snapshot versions, reading records from live segments or archived segments as needed.

## Retention and cleanup

Rapid data uses `durable-actors-v3-logs-`, under a hashed actor prefix. Startup rejects Rapid bucket deletion rules that can match this prefix. Standard manifests, segments, and current state checkpoints must remain durable.

Failed cleanup retains the Rapid copy. Logs abandoned by a crash also remain available for history; the current recovery path checkpoints their latest state but does not compact every abandoned segment. An administrative sweep for these retained copies and unreferenced empty objects is not included in this change. No lifecycle rule should delete them merely because they are old.

## Verification

The behavior tests cover stream reuse, fresh-process discovery, one-zone recovery, fencing, failed flushes, clean shutdown, archival failure, version/epoch validation, corrupted and incomplete records, rotation, large-state fallback, and ownership takeover. The live benchmark uses the repository's production Dockerfile, an optimized Rust binary, the real SDK, gVisor hosts, actor-scoped storage credentials, and GCS.

See `tests/gke-lifecycle/README.md` for the SDK lifecycle and hard-crash runners and `experiments/rapid-log/README.md` for the earlier storage-only prototype measurements.
