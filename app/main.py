import socket  # noqa: F401
import asyncio
import time
import sys
import math 
import hashlib
import struct
import os
from app import redis_proto


def encode_stream_entries(entries):
    out = f"*{len(entries)}\r\n".encode()
    for entry in entries:
        pairs = [k_or_v for k, v in entry["data"].items() for k_or_v in (k, v)]
        out += b"*2\r\n"
        out += redis_proto.encode_bulk_string(entry["id"])
        out += f"*{len(pairs)}\r\n".encode()
        for p in pairs:
            out += redis_proto.encode_bulk_string(p)
    return out

client_queues = {}  # id(writer) -> asyncio.Queue
channel_subscribers = {}  # channel -> set of writer ids
stream_waiters = {}
store = {}
expiry = {}
waiters = {}
default_user = {"nopass": True, "passwords": []}
dirty_keys = set()
key_versions = {}  # key -> version number (incremented on every write)
config = {
    "dir": os.getcwd(),
    "appendonly": "no",
    "appenddirname": "appendonlydir",
    "appendfilename": "appendonly.aof",
    "appendfsync": "everysec",
}

role = "master"
master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
master_repl_offset = 0
replica_writers = []
replica_ack_offsets = {} 
rdb_dir = ""
rdb_filename = ""


def encode_resp_array(parts):
    out = f"*{len(parts)}\r\n".encode()
    for p in parts:
        out += redis_proto.encode_bulk_string(p)
    return out


async def propagate(cmd, args):
    global master_repl_offset
    if replica_writers:
        msg = encode_resp_array([cmd] + list(args))
        master_repl_offset += len(msg)
        for w in replica_writers:
            w.write(msg)
            await w.drain()

def parse_rdb_length(data, pos):
    """Returns (length, new_pos). Handles size encoding."""
    first = data[pos]
    enc_type = (first & 0xC0) >> 6
    if enc_type == 0:
        return first & 0x3F, pos + 1
    elif enc_type == 1:
        return ((first & 0x3F) << 8) | data[pos + 1], pos + 2
    elif enc_type == 2:
        return struct.unpack(">I", data[pos+1:pos+5])[0], pos + 5
    else:
        # 0xC0-0xC3: integer encodings
        sub = first & 0x3F
        if sub == 0:
            return data[pos + 1], pos + 2
        elif sub == 1:
            return struct.unpack("<H", data[pos+1:pos+3])[0], pos + 3
        elif sub == 2:
            return struct.unpack("<I", data[pos+1:pos+5])[0], pos + 5
    return 0, pos + 1

def parse_rdb_string(data, pos):
    """Returns (string, new_pos)."""
    first = data[pos]
    enc_type = (first & 0xC0) >> 6
    if enc_type == 3:
        val, pos = parse_rdb_length(data, pos)
        return str(val), pos
    length, pos = parse_rdb_length(data, pos)
    return data[pos:pos+length].decode(), pos + length
def load_aof():
    """Replay commands from the AOF file to restore state."""
    if config.get("appendonly") != "yes":
        return
    aof_dir = os.path.join(config["dir"], config["appenddirname"])
    manifest_path = os.path.join(aof_dir, config["appendfilename"] + ".manifest")

    if not os.path.exists(manifest_path):
        return

    # Find the incremental file from manifest
    aof_file = None
    with open(manifest_path, "r") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 6 and parts[5] == "i":
                aof_file = os.path.join(aof_dir, parts[1])
                break

    if not aof_file or not os.path.exists(aof_file):
        return

    with open(aof_file, "rb") as f:
        data = f.read()

    # Parse and replay all commands
    buffer = data.decode(errors="replace")
    while buffer:
        if not buffer.startswith("*"):
            break
        lines = buffer.split("\r\n")
        try:
            num_parts = int(lines[0][1:])
        except (ValueError, IndexError):
            break

        needed = 1 + num_parts * 2
        if len(lines) < needed:
            break

        parts = []
        i = 1
        for _ in range(num_parts):
            parts.append(lines[i + 1])
            i += 2

        consumed = "\r\n".join(lines[:needed]) + "\r\n"
        buffer = buffer[len(consumed):]

        cmd, *args = parts
        if cmd.upper() == "SET":
            key, value = args[0], args[1]
            store[key] = value
            for i in range(2, len(args)):
                if args[i].upper() == "PX" and i + 1 < len(args):
                    expiry[key] = time.time() + int(args[i + 1]) / 1000
def load_rdb(filepath):
    """Load keys from RDB file into store/expiry."""
    if not os.path.exists(filepath):
        return
    with open(filepath, "rb") as f:
        data = f.read()

    pos = 9  # skip "REDIS0011"

    while pos < len(data):
        byte = data[pos]
        pos += 1

        if byte == 0xFA:  # metadata
            _, pos = parse_rdb_string(data, pos)
            _, pos = parse_rdb_string(data, pos)
        elif byte == 0xFE:  # database selector
            _, pos = parse_rdb_length(data, pos)  # db index
        elif byte == 0xFB:  # hash table sizes
            _, pos = parse_rdb_length(data, pos)
            _, pos = parse_rdb_length(data, pos)
        elif byte == 0xFF:  # end of file
            break
        else:
            # Key-value pair
            expire_ms = None
            if byte == 0xFC:  # expire in ms
                expire_ms = struct.unpack("<Q", data[pos:pos+8])[0]
                pos += 8
                byte = data[pos]; pos += 1
            elif byte == 0xFD:  # expire in seconds
                expire_ms = struct.unpack("<I", data[pos:pos+4])[0] * 1000
                pos += 4
                byte = data[pos]; pos += 1

            # byte is now value type (0 = string)
            key, pos = parse_rdb_string(data, pos)
            val, pos = parse_rdb_string(data, pos)

            now_ms = time.time() * 1000
            if expire_ms is None or expire_ms > now_ms:
                store[key] = val
                if expire_ms is not None:
                    expiry[key] = expire_ms / 1000

async def run_server():
    global role, rdb_dir, rdb_filename
    port = 6379
    argv = sys.argv[1:]

    if "--port" in argv:
        port = int(argv[argv.index("--port") + 1])
    if "--replicaof" in argv:
        role = "slave"
        master_info = argv[argv.index("--replicaof") + 1]
        master_host, master_port = master_info.split()
        asyncio.create_task(connect_to_master(master_host, int(master_port), port))
    if "--dir" in argv:
        rdb_dir = argv[argv.index("--dir") + 1]
        config["dir"] = rdb_dir
    if "--dbfilename" in argv:
        rdb_filename = argv[argv.index("--dbfilename") + 1]
    for flag in ("appendonly", "appenddirname", "appendfilename", "appendfsync"):
        if f"--{flag}" in argv:
            config[flag] = argv[argv.index(f"--{flag}") + 1]
    if config.get("appendonly") == "yes":
        aof_dir = os.path.join(config["dir"], config["appenddirname"])
        os.makedirs(aof_dir, exist_ok=True)
        aof_filename = config["appendfilename"]
        aof_file = os.path.join(aof_dir, aof_filename + ".1.incr.aof")
        manifest_file = os.path.join(aof_dir, aof_filename + ".manifest")
        
        load_aof()  # Replay FIRST before creating new files
        
        if not os.path.exists(aof_file):
            open(aof_file, "w").close()
        if not os.path.exists(manifest_file):
            with open(manifest_file, "w") as f:
                f.write(f"file {aof_filename}.1.incr.aof seq 1 type i\n")

    # Load RDB AFTER setting rdb_dir and rdb_filename
    if rdb_dir and rdb_filename:
        load_rdb(os.path.join(rdb_dir, rdb_filename))
     
    server = await asyncio.start_server(handle_client, "localhost", port)
    async with server:
        await server.serve_forever()

def _resp_length(data: bytes) -> int:
    """Returns the byte length of the first RESP value in data."""
    try:
        text = data.decode()
        lines = text.split("\r\n")
        if not lines[0].startswith("*"):
            return len(data)
        count = int(lines[0][1:])
        idx = 1
        for _ in range(count):
            length = int(lines[idx][1:])
            idx += 2 + 0  # skip $N and the value line
            idx += 1
        # reconstruct byte count
        consumed = "\r\n".join(lines[:idx]) + "\r\n"
        return len(consumed.encode())
    except Exception:
        return len(data)

async def connect_to_master(host, port, my_port):
    reader, writer = await asyncio.open_connection(host, port)

    # Step 1: PING
    writer.write(b"*1\r\n$4\r\nPING\r\n")
    await writer.drain()
    await reader.read(1024)

    # Step 2a: REPLCONF listening-port
    msg = f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(my_port))}\r\n{my_port}\r\n"
    writer.write(msg.encode())
    await writer.drain()
    await reader.read(1024)

    # Step 2b: REPLCONF capa psync2
    writer.write(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
    await writer.drain()
    await reader.read(1024)

    # Step 3: PSYNC
    writer.write(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
    await writer.drain()

    # Read FULLRESYNC line
    await reader.readline()

    # Read RDB: format is $<len>\r\n<bytes> (no trailing \r\n)
    rdb_header = await reader.readline()  # e.g. b"$88\r\n"
    rdb_len = int(rdb_header.decode().strip()[1:])
    await reader.readexactly(rdb_len)  # consume exactly the RDB bytes

    # Process propagated commands — no responses sent back
    buffer = b""
    repl_offset = 0 
    while True:
        data = await reader.read(1024)
        if not data:
            break
        buffer += data
        while True:
            try:
                text = buffer.decode()
            except Exception:
                break
            if not text.startswith("*"):
                break
            lines = text.split("\r\n")
            try:
                num_parts = int(lines[0][1:])
            except (ValueError, IndexError):
                break

            needed = 1 + num_parts * 2
            if len(lines) < needed:
                break

            parts = []
            i = 1
            for _ in range(num_parts):
                parts.append(lines[i + 1])
                i += 2

            consumed = "\r\n".join(lines[:needed]) + "\r\n"
            cmd_bytes = len(consumed.encode())
            buffer = buffer[cmd_bytes:]
            cmd, *args = parts
            match cmd.upper():
                case "SET":
                    key, value = args[0], args[1]
                    store[key] = value
                    key_versions[key] = key_versions.get(key, 0) + 1
                    for i in range(2, len(args)):
                        if args[i].upper() == "PX" and i + 1 < len(args):
                            px_ms = int(args[i + 1])
                            expiry[key] = asyncio.get_event_loop().time() + (px_ms / 1000)
                case "DEL":
                    for key in args:
                        store.pop(key, None)
                        expiry.pop(key, None)
                case "REPLCONF":
                    if args[0].upper() == "GETACK":
                        ack = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(repl_offset))}\r\n{repl_offset}\r\n"
                        writer.write(ack.encode())
                        await writer.drain()
            repl_offset += cmd_bytes

async def read_replica_acks(reader, writer):
    """Read REPLCONF ACK responses from a replica."""
    while True:
        try:
            data = await reader.read(1024)
            if not data:
                break
            parts = redis_proto.reader(data)
            if parts and parts[0].upper() == "REPLCONF" and parts[1].upper() == "ACK":
                replica_ack_offsets[id(writer)] = int(parts[2])
        except Exception:
            break   

async def execute_command(cmd, args, writer):
    match cmd.upper():
        case "SET":
            key, value = args[0], args[1]
            store[key] = value
            for i in range(2, len(args)):
                if args[i].upper() == "PX" and i + 1 < len(args):
                    px_ms = int(args[i + 1])
                    expiry[key] = asyncio.get_event_loop().time() + (px_ms / 1000)
            writer.write(redis_proto.encode_simple_string("OK"))
            await propagate("SET", args)

        case "GET":
            key = args[0]
            now = asyncio.get_event_loop().time()
            if key in expiry and now > expiry[key]:
                store.pop(key, None)
                expiry.pop(key, None)
                writer.write(redis_proto.encode_null_bulk_string())
            else:
                value = store.get(key)
                if value is None:
                    writer.write(redis_proto.encode_null_bulk_string())
                else:
                    writer.write(redis_proto.encode_bulk_string(value))

        case "INCR":
            key = args[0]
            value = store.get(key)
            if value is None:
                store[key] = "1"
                writer.write(b":1\r\n")
            else:
                try:
                    new_val = int(value) + 1
                    store[key] = str(new_val)
                    writer.write(f":{new_val}\r\n".encode())
                except ValueError:
                    writer.write(redis_proto.encode_simple_error(
                        "ERR", "value is not an integer or out of range"
                    ))

        case _:
            writer.write(redis_proto.encode_simple_error("ERR", f"Unsupported command {cmd}"))

async def deliver_messages(writer, queue):
    """Continuously deliver queued messages to a subscribed client."""
    try:
        while True:
            msg = await queue.get()
            writer.write(msg)
            await writer.drain()
    except Exception:
        pass
def geo_decode(score):
    """Decode Redis geohash score back to (lon, lat)."""
    lon_bits = 0
    lat_bits = 0
    for i in range(26):
        lon_bits |= ((score >> (51 - 2 * i)) & 1) << (25 - i)
        lat_bits |= ((score >> (51 - 2 * i - 1)) & 1) << (25 - i)

    # Decode bits back to normalized [0, 1]
    def decode_range(bits, bits_count=26):
        val = 0.0
        step = 0.5
        for i in range(bits_count):
            if (bits >> (bits_count - 1 - i)) & 1:
                val += step
            step /= 2
        val += step  # ADD THIS: move to midpoint of final cell        
        return val

    lon_norm = decode_range(lon_bits)
    lat_norm = decode_range(lat_bits)

    lon = lon_norm * 360.0 - 180.0
    lat = lat_norm * 170.10225756 - 85.05112878
    return lon, lat

def haversine(lon1, lat1, lon2, lat2):
    """Calculate distance in meters between two points using Haversine formula."""
    R = 6372797.560856
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    lat1 = math.radians(lat1)
    lat2 = math.radians(lat2)
    a = math.sin(d_lat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(d_lon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return R * c    

def geo_encode(lon, lat):
    """Encode lon/lat to Redis geohash score (52-bit interleaved)."""
    # Normalize to [0, 1]
    lon_norm = (lon + 180.0) / 360.0
    lat_norm = (lat + 85.05112878) / 170.10225756

    # Encode to 26-bit integers
    def encode_range(val, bits=26):
        result = 0
        for _ in range(bits):
            result <<= 1
            if val >= 0.5:
                result |= 1
                val = (val - 0.5) * 2
            else:
                val *= 2
        return result

    lon_bits = encode_range(lon_norm)
    lat_bits = encode_range(lat_norm)

    # Interleave: lon in even bits, lat in odd bits
    score = 0
    for i in range(26):
        score |= ((lon_bits >> (25 - i)) & 1) << (51 - 2 * i)
        score |= ((lat_bits >> (25 - i)) & 1) << (51 - 2 * i - 1)

    return score
watched_keys = set()
def aof_write(cmd, args):
    """Append a command to the AOF file if appendonly is enabled."""
    if config.get("appendonly") != "yes":
        return
    aof_dir = os.path.join(config["dir"], config["appenddirname"])
    manifest_path = os.path.join(aof_dir, config["appendfilename"] + ".manifest")
    
    # Read manifest to find active incr file
    aof_file = None
    try:
        with open(manifest_path, "r") as f:
            for line in f:
                parts = line.strip().split()
                # parts: file <name> seq <n> type <t>
                if len(parts) >= 6 and parts[5] == "i":
                    aof_file = os.path.join(aof_dir, parts[1])
                    break
    except FileNotFoundError:
        return
    
    if not aof_file:
        return
    
    # Encode command as RESP array
    resp = encode_resp_array([cmd] + list(args))
    
    with open(aof_file, "ab") as f:
        f.write(resp)
        if config.get("appendfsync") == "always":
            f.flush()
            os.fsync(f.fileno())
async def handle_client(reader, writer):
    in_multi = False
    queue = []
    subscribed_channels = set()
    authenticated = default_user["nopass"]  # start as authenticated if no password is set
    watched_keys = {} 
    watch_dirty = False         # Flag to track if any watched key was modified during MULTI
    while True:
        data = await reader.read(1024)
        parts = redis_proto.reader(data)
        if parts is None:
            break

        cmd = parts[0]
        args = parts[1:]

        NOAUTH_ALLOWED = {"AUTH", "QUIT", "HELLO"}
        if not authenticated and cmd.upper() not in NOAUTH_ALLOWED:
            writer.write(redis_proto.encode_simple_error("NOAUTH", "Authentication required."))
            await writer.drain()
            continue
        if in_multi and cmd.upper() not in ("EXEC", "DISCARD", "MULTI", "WATCH"):
            queue.append((cmd, args))
            writer.write(redis_proto.encode_simple_string("QUEUED"))
            await writer.drain()
            continue
        # If in subscribed mode, only allow certain commands
        SUBSCRIBE_ALLOWED = {"SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT", "RESET"}
        if subscribed_channels and cmd.upper() not in SUBSCRIBE_ALLOWED:
            writer.write(redis_proto.encode_simple_error(
                "ERR", f"Can't execute '{cmd.lower()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
            ))
            await writer.drain()
            continue     

        match cmd.upper():
            case "PING":
                if subscribed_channels:
                    writer.write(b"*2\r\n$4\r\npong\r\n$0\r\n\r\n")
                else:
                    writer.write(redis_proto.encode_simple_string("PONG"))
            case "ECHO":
                writer.write(redis_proto.encode_bulk_string(args[0]))

            case "SET":
                key, value = args[0], args[1]
                store[key] = value
                key_versions[key] = key_versions.get(key, 0) + 1
                for i in range(2, len(args)):
                    if args[i].upper() == "PX" and i + 1 < len(args):
                        px_ms = int(args[i + 1])
                        expiry[key] = asyncio.get_event_loop().time() + (px_ms / 1000)
                aof_write("SET", args)  # ADD THIS
                writer.write(redis_proto.encode_simple_string("OK"))
                await propagate("SET", args)

            case "RPUSH":
                key = args[0]
                elements = args[1:]
                if key not in store:
                    store[key] = []
                for el in elements:
                    store[key].append(el)
                writer.write(f":{len(store[key])}\r\n".encode())
                if key in waiters and waiters[key]:
                    event, future = waiters[key].pop(0)
                    if not future.done():
                        future.set_result(key)

            case "LPUSH":
                key = args[0]
                elements = args[1:]
                if key not in store:
                    store[key] = []
                for el in elements:
                    store[key].insert(0, el)
                writer.write(f":{len(store[key])}\r\n".encode())
                if key in waiters and waiters[key]:
                    event, future = waiters[key].pop(0)
                    if not future.done():
                        future.set_result(key)

            case "LRANGE":
                key = args[0]
                start, stop = int(args[1]), int(args[2])
                lst = store.get(key, [])
                n = len(lst)
                if start < 0:
                    start = max(0, n + start)
                if stop < 0:
                    stop = n + stop
                sliced = lst[start:stop + 1]
                writer.write(f"*{len(sliced)}\r\n".encode())
                for el in sliced:
                    writer.write(redis_proto.encode_bulk_string(el))

            case "LLEN":
                key = args[0]
                lst = store.get(key, [])
                writer.write(f":{len(lst)}\r\n".encode())

            case "LPOP":
                key = args[0]
                lst = store.get(key)
                if not lst:
                    writer.write(redis_proto.encode_null_bulk_string())
                elif len(args) > 1:
                    count = min(int(args[1]), len(lst))
                    popped = [lst.pop(0) for _ in range(count)]
                    writer.write(f"*{len(popped)}\r\n".encode())
                    for el in popped:
                        writer.write(redis_proto.encode_bulk_string(el))
                else:
                    writer.write(redis_proto.encode_bulk_string(lst.pop(0)))

            case "BLPOP":
                key = args[0]
                timeout = float(args[1])
                lst = store.get(key)
                if lst:
                    el = lst.pop(0)
                    writer.write(b"*2\r\n")
                    writer.write(redis_proto.encode_bulk_string(key))
                    writer.write(redis_proto.encode_bulk_string(el))
                else:
                    future = asyncio.get_event_loop().create_future()
                    if key not in waiters:
                        waiters[key] = []
                    waiters[key].append((None, future))
                    await writer.drain()
                    try:
                        await asyncio.wait_for(future, timeout=timeout if timeout > 0 else None)
                        lst = store.get(key, [])
                        if lst:
                            el = lst.pop(0)
                            writer.write(b"*2\r\n")
                            writer.write(redis_proto.encode_bulk_string(key))
                            writer.write(redis_proto.encode_bulk_string(el))
                        else:
                            writer.write(b"*-1\r\n")
                    except asyncio.TimeoutError:
                        if key in waiters:
                            waiters[key] = [(e, f) for e, f in waiters[key] if f is not future]
                        writer.write(b"*-1\r\n")

            case "TYPE":
                key = args[0]
                if key not in store:
                    writer.write(redis_proto.encode_simple_string("none"))
                elif isinstance(store[key], dict) and store[key].get("type") == "stream":
                    writer.write(redis_proto.encode_simple_string("stream"))
                elif isinstance(store[key], list):
                    writer.write(redis_proto.encode_simple_string("list"))
                else:
                    writer.write(redis_proto.encode_simple_string("string"))

            case "XADD":
                key = args[0]
                entry_id = args[1]
                pairs = {}
                for i in range(2, len(args), 2):
                    pairs[args[i]] = args[i + 1]

                last_ms, last_seq = 0, 0
                if key in store and store[key].get("entries"):
                    last_id = store[key]["entries"][-1]["id"]
                    last_ms, last_seq = map(int, last_id.split("-"))

                if entry_id == "*":
                    ms = int(time.time() * 1000)
                    seq = last_seq + 1 if ms == last_ms else 0
                    entry_id = f"{ms}-{seq}"
                elif entry_id.endswith("-*"):
                    ms = int(entry_id.split("-")[0])
                    if ms == last_ms:
                        seq = last_seq + 1
                    else:
                        seq = 1 if ms == 0 else 0
                    entry_id = f"{ms}-{seq}"
                else:
                    ms, seq = map(int, entry_id.split("-"))

                if ms == 0 and seq == 0:
                    writer.write(redis_proto.encode_simple_error(
                        "ERR", "The ID specified in XADD must be greater than 0-0"
                    ))
                elif (ms, seq) <= (last_ms, last_seq):
                    writer.write(redis_proto.encode_simple_error(
                        "ERR", "The ID specified in XADD is equal or smaller than the target stream top item"
                    ))
                else:
                    if key not in store:
                        store[key] = {"type": "stream", "entries": []}
                    store[key]["entries"].append({"id": entry_id, "data": pairs})
                    writer.write(redis_proto.encode_bulk_string(entry_id))
                    if key in stream_waiters:
                        for future in stream_waiters.pop(key, []):
                            if not future.done():
                                future.set_result(key)

            case "GET":
                key = args[0]
                now = asyncio.get_event_loop().time()
                if key in expiry and now > expiry[key]:
                    store.pop(key, None)
                    expiry.pop(key, None)
                    writer.write(redis_proto.encode_null_bulk_string())
                else:
                    value = store.get(key)
                    if value is None:
                        writer.write(redis_proto.encode_null_bulk_string())
                    else:
                        writer.write(redis_proto.encode_bulk_string(value))

            case "INCR":
                key = args[0]
                value = store.get(key)
                if value is None:
                    store[key] = "1"
                    writer.write(b":1\r\n")
                else:
                    try:
                        new_val = int(value) + 1
                        store[key] = str(new_val)
                        writer.write(f":{new_val}\r\n".encode())
                    except ValueError:
                        writer.write(redis_proto.encode_simple_error(
                            "ERR", "value is not an integer or out of range"
                        ))

            case "XRANGE":
                key = args[0]
                start, end = args[1], args[2]
                if start == "-":
                    s_ms, s_seq = 0, 0
                elif "-" in start:
                    s_ms, s_seq = map(int, start.split("-"))
                else:
                    s_ms, s_seq = int(start), 0
                if end == "+":
                    e_ms, e_seq = float("inf"), float("inf")
                elif "-" in end:
                    e_ms, e_seq = map(int, end.split("-"))
                else:
                    e_ms, e_seq = int(end), float("inf")
                entries = store.get(key, {}).get("entries", [])
                result = [
                    e for e in entries
                    if (s_ms, s_seq) <= tuple(int(x) for x in e["id"].split("-")) <= (e_ms, e_seq)
                ]
                writer.write(encode_stream_entries(result))

            case "XREAD":
                upper_args = [a.upper() for a in args]
                block_ms = None
                if "BLOCK" in upper_args:
                    block_idx = upper_args.index("BLOCK")
                    block_ms = int(args[block_idx + 1])
                streams_idx = upper_args.index("STREAMS") + 1
                remaining = args[streams_idx:]
                mid = len(remaining) // 2
                read_keys = remaining[:mid]
                read_ids = remaining[mid:]

                def resolve_id(k, start_id):
                    if start_id == "$":
                        ents = store.get(k, {}).get("entries", [])
                        return ents[-1]["id"] if ents else "0-0"
                    return start_id

                def read_entries(k, start_id):
                    ents = store.get(k, {}).get("entries", [])
                    if "-" in start_id:
                        s_ms, s_seq = map(int, start_id.split("-"))
                    else:
                        s_ms, s_seq = int(start_id), 0
                    return [
                        e for e in ents
                        if tuple(int(x) for x in e["id"].split("-")) > (s_ms, s_seq)
                    ]

                resolved_ids = [resolve_id(k, i) for k, i in zip(read_keys, read_ids)]

                if block_ms is not None:
                    results = [(k, read_entries(k, i)) for k, i in zip(read_keys, resolved_ids)]
                    has_data = any(r for _, r in results)
                    if not has_data:
                        futures = []
                        for k in read_keys:
                            future = asyncio.get_event_loop().create_future()
                            if k not in stream_waiters:
                                stream_waiters[k] = []
                            stream_waiters[k].append(future)
                            futures.append(future)
                        await writer.drain()
                        timeout = block_ms / 1000 if block_ms > 0 else None
                        try:
                            await asyncio.wait_for(
                                asyncio.ensure_future(asyncio.gather(*futures, return_exceptions=True)),
                                timeout=timeout
                            )
                        except asyncio.TimeoutError:
                            for k in read_keys:
                                if k in stream_waiters:
                                    stream_waiters[k] = [f for f in stream_waiters[k] if not f.done()]
                            writer.write(b"*-1\r\n")
                            await writer.drain()
                            continue
                        results = [(k, read_entries(k, i)) for k, i in zip(read_keys, resolved_ids)]
                    results = [(k, r) for k, r in results if r]
                    if not results:
                        writer.write(b"*-1\r\n")
                    else:
                        out = f"*{len(results)}\r\n".encode()
                        for k, result in results:
                            out += b"*2\r\n"
                            out += redis_proto.encode_bulk_string(k)
                            out += encode_stream_entries(result)
                        writer.write(out)
                else:
                    out = f"*{len(read_keys)}\r\n".encode()
                    for k, start_id in zip(read_keys, resolved_ids):
                        result = read_entries(k, start_id)
                        out += b"*2\r\n"
                        out += redis_proto.encode_bulk_string(k)
                        out += encode_stream_entries(result)
                    writer.write(out)

            case "MULTI":
                in_multi = True
                writer.write(redis_proto.encode_simple_string("OK"))

            case "EXEC":
                if not in_multi:
                    writer.write(redis_proto.encode_simple_error("ERR", "EXEC without MULTI"))
                else:
                    in_multi = False
                    dirty = any(
                        key_versions.get(k, 0) != v
                        for k, v in watched_keys.items()
                    )
                    if dirty:
                        writer.write(b"*-1\r\n")
                    else:
                        writer.write(f"*{len(queue)}\r\n".encode())
                        for q_cmd, q_args in queue:
                            await execute_command(q_cmd, q_args, writer)
                    queue.clear()
                    watched_keys.clear()
            case "DISCARD":
                if not in_multi:
                    writer.write(redis_proto.encode_simple_error("ERR", "DISCARD without MULTI"))
                else:
                    in_multi = False
                    queue.clear()
                    watched_keys.clear()
                    watch_dirty = False
                    writer.write(redis_proto.encode_simple_string("OK"))

            case "INFO":
                info = "\r\n".join([
                    f"role:{role}",
                    f"master_replid:{master_replid}",
                    f"master_repl_offset:{master_repl_offset}",
                ])
                writer.write(redis_proto.encode_bulk_string(info))

            case "REPLCONF":
                writer.write(redis_proto.encode_simple_string("OK"))

            case "PSYNC":
                writer.write(redis_proto.encode_simple_string(f"FULLRESYNC {master_replid} 0"))
                empty_rdb = bytes.fromhex(
                    "524544495330303131fa0972656469732d76657205372e322e30"
                    "fa0a72656469732d62697473c040fa056374696d65c26d08bc65"
                    "fa08757365642d6d656dc2b0c41000fa08616f662d62617365c0"
                    "00fff06e3bfec0ff5aa2"
                )
                writer.write(f"${len(empty_rdb)}\r\n".encode() + empty_rdb)
                await writer.drain()
                replica_writers.append(writer)
                replica_ack_offsets[id(writer)] = 0
                asyncio.create_task(read_replica_acks(reader, writer))
                await asyncio.Future()
                return
            case "WAIT":
                num_replicas = int(args[0])
                timeout_ms = int(args[1])

                if master_repl_offset == 0 or not replica_writers:
                    writer.write(f":{len(replica_writers)}\r\n".encode())
                else:
                    # Send GETACK to all replicas
                    getack = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
                    for w in replica_writers:
                        w.write(getack)
                        await w.drain()

                    # Wait until enough replicas ACK or timeout
                    deadline = asyncio.get_event_loop().time() + timeout_ms / 1000
                    while True:
                        acked = sum(
                            1 for w in replica_writers
                            if replica_ack_offsets.get(id(w), 0) >= master_repl_offset
                        )
                        if acked >= num_replicas:
                            break
                        if asyncio.get_event_loop().time() >= deadline:
                            break
                        await asyncio.sleep(0.05)

                    acked = sum(
                        1 for w in replica_writers
                        if replica_ack_offsets.get(id(w), 0) >= master_repl_offset
                    )
                    writer.write(f":{acked}\r\n".encode())
            case "CONFIG":
                if args[0].upper() == "GET":
                    param = args[1].lower()
                    # legacy handling for rdb_dir/rdb_filename
                    if param == "dbfilename":
                        val = rdb_filename
                    else:
                        val = config.get(param, "")
                    writer.write(b"*2\r\n")
                    writer.write(redis_proto.encode_bulk_string(param))
                    writer.write(redis_proto.encode_bulk_string(val))
            case "KEYS":
                pattern = args[0]
                if pattern == "*":
                    keys = list(store.keys())
                else:
                    keys = [k for k in store if k.startswith(pattern.rstrip("*"))]
                writer.write(f"*{len(keys)}\r\n".encode())
                for k in keys:
                    writer.write(redis_proto.encode_bulk_string(k)) 
            case "SUBSCRIBE":
                if id(writer) not in client_queues:
                    client_queues[id(writer)] = asyncio.Queue()
                for channel in args:
                    subscribed_channels.add(channel)
                    if channel not in channel_subscribers:
                        channel_subscribers[channel] = []
                    if writer not in channel_subscribers[channel]:
                        channel_subscribers[channel].append(writer)
                    count = len(subscribed_channels)
                    writer.write(
                        f"*3\r\n$9\r\nsubscribe\r\n${len(channel)}\r\n{channel}\r\n:{count}\r\n".encode()
                    )
                await writer.drain()

                # Start listening for messages if not already doing so
                if len(subscribed_channels) == len(args):  # first SUBSCRIBE
                    asyncio.create_task(deliver_messages(writer, client_queues[id(writer)]))
            case "PUBLISH":
                channel, message = args[0], args[1]
                subscribers = channel_subscribers.get(channel, [])
                msg = (
                    f"*3\r\n$7\r\nmessage\r\n"
                    f"${len(channel)}\r\n{channel}\r\n"
                    f"${len(message)}\r\n{message}\r\n"
                ).encode()
                for w in subscribers:
                    q = client_queues.get(id(w))
                    if q:
                        await q.put(msg)
                writer.write(f":{len(subscribers)}\r\n".encode())
            case "UNSUBSCRIBE":
                for channel in args:
                    subscribed_channels.discard(channel)
                    if channel in channel_subscribers and writer in channel_subscribers[channel]:
                        channel_subscribers[channel].remove(writer)
                    count = len(subscribed_channels)
                    writer.write(
                        f"*3\r\n$11\r\nunsubscribe\r\n${len(channel)}\r\n{channel}\r\n:{count}\r\n".encode()
                    )
            case "ZADD":
                key = args[0]
                score = float(args[1])
                member = args[2]

                if key not in store:
                    store[key] = []  # list of (score, member) tuples, kept sorted

                # Check if member already exists
                existing = [(s, m) for s, m in store[key] if m == member]
                if existing:
                    # Update score
                    store[key] = [(s, m) if m != member else (score, member) for s, m in store[key]]
                    store[key].sort(key=lambda x: (x[0], x[1]))
                    writer.write(b":0\r\n")
                else:
                    store[key].append((score, member))
                    store[key].sort(key=lambda x: (x[0], x[1]))
                    writer.write(b":1\r\n")    
            case "ZRANK":
                key = args[0]
                member = args[1]
                zset = store.get(key)
                if not zset or not isinstance(zset, list):
                    writer.write(redis_proto.encode_null_bulk_string())
                else:
                    for i, (score, m) in enumerate(zset):
                        if m == member:
                            writer.write(f":{i}\r\n".encode())
                            break
                    else:
                        writer.write(redis_proto.encode_null_bulk_string())     

            case "ZRANGE":
                key = args[0]
                start, stop = int(args[1]), int(args[2])
                zset = store.get(key, [])
                n = len(zset)

                if start < 0:
                    start = max(0, n + start)
                if stop < 0:
                    stop = n + stop

                sliced = zset[start:stop + 1]
                writer.write(f"*{len(sliced)}\r\n".encode())
                for score, member in sliced:
                    writer.write(redis_proto.encode_bulk_string(member))

            case "ZCARD":
                key = args[0]
                zset = store.get(key, [])
                writer.write(f":{len(zset)}\r\n".encode())
            case "ZSCORE":
                key = args[0]
                member = args[1]
                zset = store.get(key)
                if not zset:
                    writer.write(redis_proto.encode_null_bulk_string())
                else:
                    for score, m in zset:
                        if m == member:
                            # Format score: remove trailing zeros but keep decimals
                            score_str = repr(score)
                            writer.write(redis_proto.encode_bulk_string(score_str))
                            break
                    else:
                        writer.write(redis_proto.encode_null_bulk_string())
            case "ZREM":
                key = args[0]
                member = args[1]
                zset = store.get(key, [])
                new_zset = [x for x in zset if x[1] != member]
                removed = len(zset) - len(new_zset)
                store[key] = new_zset
                writer.write(f":{removed}\r\n".encode()) 
            case "GEOADD":
                key = args[0]
                lon = float(args[1])
                lat = float(args[2])
                member = args[3]

                if not (-180 <= lon <= 180):
                    writer.write(redis_proto.encode_simple_error("ERR", f"invalid longitude value {lon}"))
                elif not (-85.05112878 <= lat <= 85.05112878):
                    writer.write(redis_proto.encode_simple_error("ERR", f"invalid latitude value {lat}"))
                else:
                    score = geo_encode(lon, lat)
                    if key not in store:
                        store[key] = []
                    existing = [x for x in store[key] if x[1] == member]
                    if existing:
                        store[key] = [(score, m) if m == member else (s, m) for s, m in store[key]]
                        store[key].sort(key=lambda x: (x[0], x[1]))
                        writer.write(b":0\r\n")
                    else:
                        store[key].append((score, member))
                        store[key].sort(key=lambda x: (x[0], x[1]))
                        writer.write(b":1\r\n")
            case "GEOPOS":
                key = args[0]
                members = args[1:]
                zset = store.get(key, [])
                score_map = {m: s for s, m in zset}

                writer.write(f"*{len(members)}\r\n".encode())
                for member in members:
                    if member in score_map:
                        lon, lat = geo_decode(int(score_map[member]))
                        lon_str = repr(lon)
                        lat_str = repr(lat)
                        writer.write(b"*2\r\n")
                        writer.write(redis_proto.encode_bulk_string(lon_str))
                        writer.write(redis_proto.encode_bulk_string(lat_str))
                    else:
                        writer.write(b"*-1\r\n")
            case "GEODIST":
                key = args[0]
                member1, member2 = args[1], args[2]
                zset = store.get(key, [])
                score_map = {m: s for s, m in zset}

                if member1 not in score_map or member2 not in score_map:
                    writer.write(redis_proto.encode_null_bulk_string())
                else:
                    lon1, lat1 = geo_decode(int(score_map[member1]))
                    lon2, lat2 = geo_decode(int(score_map[member2]))
                    dist = haversine(lon1, lat1, lon2, lat2)
                    dist_str = f"{dist:.4f}"
                    writer.write(redis_proto.encode_bulk_string(dist_str))
            case "GEOSEARCH":
                key = args[0]
                upper = [a.upper() for a in args]

                # Parse FROMLONLAT
                fl_idx = upper.index("FROMLONLAT")
                center_lon = float(args[fl_idx + 1])
                center_lat = float(args[fl_idx + 2])

                # Parse BYRADIUS
                br_idx = upper.index("BYRADIUS")
                radius = float(args[br_idx + 1])
                unit = args[br_idx + 2].lower()

                # Convert radius to meters
                unit_multipliers = {"m": 1, "km": 1000, "mi": 1609.344, "ft": 0.3048}
                radius_m = radius * unit_multipliers.get(unit, 1)

                zset = store.get(key, [])
                results = []
                for score, member in zset:
                    lon, lat = geo_decode(int(score))
                    dist = haversine(center_lon, center_lat, lon, lat)
                    if dist <= radius_m:
                        results.append(member)

                writer.write(f"*{len(results)}\r\n".encode())
                for member in results:
                    writer.write(redis_proto.encode_bulk_string(member))  
            case "ACL":
                if args[0].upper() == "WHOAMI":
                    writer.write(redis_proto.encode_bulk_string("default"))

                elif args[0].upper() == "GETUSER":
                    flags = ["nopass"] if default_user["nopass"] else []
                    writer.write(b"*4\r\n")
                    writer.write(redis_proto.encode_bulk_string("flags"))
                    writer.write(f"*{len(flags)}\r\n".encode())
                    for f in flags:
                        writer.write(redis_proto.encode_bulk_string(f))
                    writer.write(redis_proto.encode_bulk_string("passwords"))
                    writer.write(f"*{len(default_user['passwords'])}\r\n".encode())
                    for p in default_user["passwords"]:
                        writer.write(redis_proto.encode_bulk_string(p))

                elif args[0].upper() == "SETUSER":
                    for rule in args[2:]:
                        if rule.startswith(">"):
                            password = rule[1:]
                            hashed = hashlib.sha256(password.encode()).hexdigest()
                            if hashed not in default_user["passwords"]:
                                default_user["passwords"].append(hashed)
                            default_user["nopass"] = False
                    writer.write(redis_proto.encode_simple_string("OK"))
            case "AUTH":
                username = args[0]
                password = args[1]
                hashed = hashlib.sha256(password.encode()).hexdigest()
                if default_user["nopass"] or hashed in default_user["passwords"]:
                    authenticated = True
                    writer.write(redis_proto.encode_simple_string("OK"))
                else:
                    writer.write(redis_proto.encode_simple_error(
                        "WRONGPASS", "invalid username-password pair or user is disabled."
                    )) 
            case "WATCH":
                if in_multi:
                    writer.write(redis_proto.encode_simple_error(
                        "ERR", "WATCH inside MULTI is not allowed"
                    ))
                else:
                    for key in args:
                        watched_keys[key] = key_versions.get(key, 0)
                    writer.write(redis_proto.encode_simple_string("OK"))
            case "UNWATCH":
                watched_keys.clear()
                writer.write(redis_proto.encode_simple_string("OK"))        
                    
            case _:
                writer.write(
                    redis_proto.encode_simple_error("ERR", f"Unsupported command {cmd}")
                )
            
        await writer.drain()

    writer.close()


if __name__ == "__main__":
    asyncio.run(run_server())