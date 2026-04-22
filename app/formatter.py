class ResponseFormatter:
    def format(self, value):
        if value is None:
            return b"$-1\r\n"   # null bulk string

        return f"${len(value)}\r\n{value}\r\n".encode()

    def error(self, message):
        return f"-{message}\r\n".encode()
