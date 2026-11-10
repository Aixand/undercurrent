import asyncio
import sys
from undercurrent import Client
from demo_helpers import wait_for_line

async def main():
    async with Client() as client:
        session = await client.start(
            [sys.executable, "-u", "worker.py", "--interactive"],
            name="python-demo", detection=False,
        )
        await wait_for_line(client, session, "READY: waiting for input")
        print("Started: python-demo", flush=True)
        await client.send_input(session, "hello from Python", paste=False)
        print(await wait_for_line(client, session, "APP_ACK: hello from Python"), flush=True)
        await client.stop(session)
        stopped = await client.wait(session, lifecycle={"stopped", "exited"}, timeout=10)
        print(f"Final state: {stopped.lifecycle}", flush=True)

asyncio.run(main())
