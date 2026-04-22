import asyncio
from app.formatter import ResponseFormatter
from app.parser import CommandParser

formater = ResponseFormatter()

# Global stores
store = {}
expiry = {}


class RedisServer:
    def __init__(self, reader, writer) -> None:
        self._reader = reader
        self._writer = writer
        self._commands = {
            "ping": self._ping,
            "echo": self._echo,
            "set": self._set,
            "get": self._get,
        }

    async def serve(self):
        addr = self._writer.get_extra_info("peername")

        while True:
            data = await self._reader.read(1024)
            if not data:
                break

            command, *args = CommandParser(data).parse()
            command = command.casefold()

            try:
                if command in self._commands:
                    self._commands[command](*args)
                else:
                    self._writer.write(formater.format(None))

            except Exception as e:
                err = formater.error(f"ERR on '{command}' command: {e}")
                self._writer.write(err)

            await self._writer.drain()

        self._writer.close()

    # ---------------- COMMANDS ---------------- #

    def _ping(self, *args):
        self._writer.write(formater.format("PONG"))

    def _echo(self, value):
        self._writer.write(formater.format(value))

    # 🔥 FINAL FIXED SET
    def _set(self, key, value, *args):
        store[key] = value

        # Robust PX handling (scan all args)
        for i in range(len(args)):
            if args[i].lower() == "px" and i + 1 < len(args):
                px_value = int(args[i + 1])
                expiry[key] = asyncio.get_running_loop().time() + (px_value / 1000)

        self._writer.write(formater.format("OK"))

    # 🔥 FINAL FIXED GET
    def _get(self, key):
        now = asyncio.get_running_loop().time()

        # Lazy expiration
        if key in expiry:
            if now > expiry[key]:
                store.pop(key, None)
                expiry.pop(key, None)
                self._writer.write(formater.format(None))
                return

        value = store.get(key)
        self._writer.write(formater.format(value))
