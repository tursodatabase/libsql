# libSQL Sync Protocol Specification

## Overview

This is a protocol for supporting offline writes by allowing a database instance to sync its write-ahead log between clients and a remote server.

## Operations

### PushWAL

Push the local WAL to a remote server.

**Request:**

- `database_id`: The ID of the database.
- `checkpoint_seq_num`: The current checkpoint sequence number.
- `frame_num_start`: The number of the first frame to push.
- `frame_num_end`: The number of the first frame to push.
- `frames`: The WAL frames to push.

**Response:**

- `status`: SUCCESS, CONFLICT, ERROR, or NEED_FULL_SYNC
- `durable_frame_num`: The highest frame number the server acknowledges as durable.

A client uses the `PushWAL` operation to push its local WAL to the remote server. The operation is idempotent on frames, which means it is safe for the client to send the same frames multiple times. If the server already has them, it ignores them. As an optimization, the client can keep track of durable checkpoint sequence and frame number tuple acknowledged by a remote server to prevent sending duplicate frames.

**TODO:**

- Return remote WAL on conflict if client requests it.
- Allow client to request server to perform checkpointing.
- Checksum support in the WAL frames.

### PullWAL

Retrieve new WAL frames from the remote server.

**Request**:

- `database_id`: The ID of the database.
- `checkpoint_seq_num`: The current checkpoint sequence number.
- `max_frame_num`: The highest frame number in the local WAL.

**Response**:
- `status`: SUCCESS, CONFLICT, ERROR, or NEED_FULL_SYNC
- `frames`: List of new WAL frames

### FetchDatabase

Retrieve the full database file from the server.

**Request**: 

- `database_id`: The ID of the database.

**Response**:

- Stream of database chunks

A client uses the `FetchDatabase` operation to bootstrap a database file locally and also for disaster recovery.

## Checkpointing Process

1. Client may request a checkpoint during PushWAL.
2. Server decides whether to initiate a checkpoint based on its state and the client's request.
3. If checkpoint is needed, server sets `perform_checkpoint` to true in the PushWAL response.
4. Client performs local checkpoint up to `checkpoint_frame_id` if instructed.
5. Server performs its own checkpoint after sending the response.

## Conflict Resolution

- The server returns `CONFLICT` error if the WAL on remote is more up-to-date than the client. 
- The server sends its current WAL in the response for the client to merge and retry the push.

## Bootstrapping

1. New clients start by calling `FetchDatabase` to get the full database file.
2. Follow up with PullWAL to get any new changes since the database file was generated.
3. Apply received WAL frames to the database file to reach the current state.
