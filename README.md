# Redis Clone

A Redis server built from scratch in async Python, implementing the RESP2 protocol over TCP.

## Features

### Core Protocol
- RESP2 encoding/decoding over raw TCP
- Concurrent client handling via `asyncio`
- Pipelined command processing

### Commands

| Category | Commands |
|---|---|
| Strings | `SET` (with `PX` expiry), `GET`, `INCR` |
| Lists | `LPUSH`, `RPUSH`, `LPOP` (with count), `BLPOP` (blocking + timeout), `LRANGE`, `LLEN` |
| Sorted Sets | `ZADD`, `ZRANK`, `ZRANGE`, `ZCARD`, `ZSCORE`, `ZREM` |
| Streams | `XADD` (explicit, auto-seq, auto-id), `XRANGE`, `XREAD` (blocking + `$`) |
| Pub/Sub | `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, `PING` (in subscribed mode) |
| Geospatial | `GEOADD`, `GEOPOS`, `GEODIST`, `GEOSEARCH` |
| Transactions | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` |
| Server | `PING`, `ECHO`, `INFO`, `CONFIG GET`, `KEYS`, `TYPE`, `WAIT` |
| Auth | `AUTH`, `ACL WHOAMI`, `ACL GETUSER`, `ACL SETUSER` |

### Data Persistence
- **RDB** — loads snapshots from disk at startup (`--dir`, `--dbfilename`)
- **AOF** — appends write commands to an incremental file; replays on startup to restore state (`--appendonly`, `--appenddirname`, `--appendfilename`, `--appendfsync`)

### Replication
- Master/replica handshake (PING → REPLCONF → PSYNC)
- Full resynchronization via empty RDB file
- Write command propagation to all connected replicas
- Replication offset tracking with `REPLCONF GETACK`
- `WAIT` command with ACK-based synchronization and timeout

### Transactions & Optimistic Locking
- `MULTI`/`EXEC`/`DISCARD` for queued command execution
- `WATCH` tracks key versions — aborts transaction if a watched key is modified by another client before `EXEC`
- `UNWATCH` clears watch state

### Authentication
- Per-connection auth state
- `ACL SETUSER` stores SHA-256 password hashes
- `NOAUTH` enforcement on unauthenticated connections
- `nopass` flag support

### Geospatial
- Hand-rolled 52-bit interleaved geohash encoder and decoder (no libraries)
- Haversine distance formula using Redis's exact Earth radius (`6372797.560856 m`)
- `GEOSEARCH` with `FROMLONLAT` + `BYRADIUS` in m/km/mi/ft

## Usage

```bash
# Start as master on default port
./your_program.sh

# Custom port
./your_program.sh --port 6380

# Start as replica
./your_program.sh --port 6380 --replicaof "localhost 6379"

# With RDB persistence
./your_program.sh --dir /tmp/redis-data --dbfilename dump.rdb

# With AOF persistence
./your_program.sh --dir /tmp/redis-data --appendonly yes --appenddirname appendonlydir --appendfilename appendonly.aof --appendfsync always
```

## Project Structure

```
app/
├── main.py          # Server entrypoint, all command handlers
├── redis_proto.py   # RESP2 encoder/decoder
├── parser.py        # Command parser
├── formatter.py     # Response formatter
└── store.py         # Key/value store helpers
```

## Implementation Notes

- All I/O is non-blocking using Python's `asyncio` — each client runs as a coroutine
- Blocking commands (`BLPOP`, `XREAD BLOCK`) use `asyncio.Future` objects that are resolved when data arrives
- Pub/Sub delivery uses per-client `asyncio.Queue` with a background delivery coroutine
- Replica connections are kept alive indefinitely using `await asyncio.Future()` after the handshake
- AOF writes are flushed with `os.fsync` when `appendfsync always` is set

## Requirements

- Python 3.14+
- [`uv`](https://github.com/astral-sh/uv) (used by `your_program.sh`)