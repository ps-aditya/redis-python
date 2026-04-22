class CommandParser:
    def __init__(self, data: bytes):
        self.data = data.decode()

    def parse(self):
        parts = self.data.split("\r\n")
        result = []

        i = 0
        while i < len(parts):
            if parts[i].startswith("$"):
                result.append(parts[i + 1])
                i += 2
            else:
                i += 1

        return result
