"""Turn a Carrier request into an HTTP build trigger and a correlated reply."""
import argparse
import asyncio
import json
import os
import re
import urllib.request

from carrier import Client


def trigger(endpoint, task_id):
    # The destination must retain this key and return the same result on retry.
    request = urllib.request.Request(
        endpoint,
        data=json.dumps({"task_id": task_id}).encode(),
        headers={"Content-Type": "application/json", "Idempotency-Key": task_id},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        result = json.load(response)
    if not isinstance(result, dict) or not isinstance(result.get("job_id"), str):
        raise ValueError("API response must contain a string job_id")
    return {"task_id": task_id, "status": "accepted", "job_id": result["job_id"]}


async def main(args):
    async with Client(url=args.url, credentials=args.credentials) as inbox:
        if args.mode == "worker":
            @inbox.consumer()
            async def handle(message):
                try:
                    body = message.json()
                    valid = (isinstance(body, dict) and body.get("operation") == "build"
                             and isinstance(body.get("task_id"), str)
                             and re.fullmatch(r"[A-Za-z0-9_-]{1,80}", body["task_id"]))
                except ValueError:
                    valid = False
                if not valid:
                    result = {"status": "rejected", "reason": "Expected build and a valid task_id"}
                else:
                    print(f"Request {message.id}: POST build for {body['task_id']}", flush=True)
                    # An API/transport failure stops the consumer and leaves the request pending.
                    result = await asyncio.to_thread(trigger, args.endpoint, body["task_id"])
                await message.reply_json(result, idempotency_key=f"api-result:{message.id}")
                await message.ack()
                print(f"Reply to {message.id}: {json.dumps(result)}", flush=True)
                print("Request acknowledged", flush=True)

            print(f"Listening as {inbox.address}", flush=True)
            await inbox.run()
        else:
            request = await inbox.send_json(
                {"operation": "build", "task_id": args.task_id},
                to=args.to, idempotency_key=f"build:{args.task_id}",
            )
            print(f"Stored request {request.id}; waiting for its API result", flush=True)
            # Include acknowledged history so rerunning a completed request is useful too.
            history = await inbox.receive(reply_to=request.id, limit=1)
            try:
                answer = (history.messages[0] if history.messages else
                          await inbox.wait_for_reply(request, timeout=args.timeout))
            except TimeoutError:
                raise SystemExit(f"Request {request.id} is still stored. Retry the same task ID later.") from None
            result = answer.json()
            print(json.dumps(result, indent=2), flush=True)
            await answer.ack()
            if result.get("status") != "accepted":
                raise SystemExit("The API request was rejected.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["worker", "request"])
    parser.add_argument("--url", default=os.environ.get("CARRIER_URL", "http://127.0.0.1:8877"))
    parser.add_argument("--credentials", required=True)
    parser.add_argument("--endpoint", default=os.environ.get("API_ENDPOINT", "http://127.0.0.1:9090/builds"))
    parser.add_argument("--to", default="services/build")
    parser.add_argument("--task-id", default="build-001")
    parser.add_argument("--timeout", type=float, default=30)
    try:
        asyncio.run(main(parser.parse_args()))
    except KeyboardInterrupt:
        pass
