"""Bounded output verification for the recording's sample application."""
import asyncio

async def wait_for_line(client, session, marker):
    offset, retained = 0, b""
    async with asyncio.timeout(10):
        while True:
            page = await client.read_output(session, from_offset=offset, max_bytes=4096)
            if page.from_offset != offset:
                raise RuntimeError("The demo output interval is discontinuous")
            retained += page.data
            for line in retained.decode("utf-8", errors="replace").splitlines():
                if line == marker:
                    return line
            if len(retained) > 32768:
                raise RuntimeError("The demo exceeded its output budget")
            # next_offset is the producer head; advance only by bytes actually read.
            offset = page.from_offset + len(page.data)
            await asyncio.sleep(0.05)
