# Durable CF Streams

durable-cf-streams provides Cloudflare-oriented storage adapters and protocol utilities for durable streams. This context names the storage-layout concepts used when deciding how stream bytes are persisted behind the public stream interface.

## Language

**Stream Store**:
The storage adapter interface for durable stream bytes and stream metadata.
_Avoid_: backend, database wrapper

**Snapshot Layout**:
A persistence layout that stores all bytes for a stream in one row, key, value, or object.
_Avoid_: one-blob store

**Chunked SQL Layout**:
A SQLite or D1-oriented persistence layout that stores stream metadata separately from bounded stream chunks.
_Avoid_: app-level chunking, Nexus chunking

**Stream Chunk**:
A contiguous byte range for one stream, stored below the backend's row or value limit. It is not a protocol event, JSON item, or application message.
_Avoid_: event, message

**Legacy Prefix**:
Existing snapshot bytes read before chunk rows when a chunked store opens an old `streams.data` row.
_Avoid_: lazy migration
