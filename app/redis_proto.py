# redis_proto.py

def reader(data: bytes):
    """
    Parses a RESP Array like:
    *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
    Returns: ["GET", "foo"]
    """
    try:
        text = data.decode()
        parts = text.split("\r\n")

        if not parts[0].startswith("*"):
            return None

        result = []
        i = 1
        while i < len(parts) and parts[i]:
            if parts[i].startswith("$"):
                result.append(parts[i + 1])
                i += 2
            else:
                i += 1

        return result
    except Exception:
        return None


def encode_simple_string(value: str) -> bytes:
    return f"+{value}\r\n".encode()


def encode_bulk_string(value: str) -> bytes:
    return f"${len(value)}\r\n{value}\r\n".encode()


def encode_null_bulk_string() -> bytes:
    return b"$-1\r\n"


def encode_simple_error(prefix: str, message: str) -> bytes:
    return f"-{prefix} {message}\r\n".encode()
