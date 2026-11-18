"""A local build API with durable idempotency, for the local integration walkthrough."""
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
import sqlite3

database = sqlite3.connect("builds.db")
database.execute("CREATE TABLE IF NOT EXISTS jobs (key TEXT PRIMARY KEY, body TEXT, result TEXT)")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ready")

    def do_POST(self):
        if self.path != "/builds":
            self.send_error(404)
            return
        key = self.headers["Idempotency-Key"]
        body = self.rfile.read(int(self.headers["Content-Length"])).decode()
        existing = database.execute("SELECT body, result FROM jobs WHERE key = ?", (key,)).fetchone()
        if existing and existing[0] != body:
            self.send_error(409)
            return
        result = existing[1] if existing else json.dumps({"job_id": "job-" + key})
        if not existing:
            database.execute("INSERT INTO jobs VALUES (?, ?, ?)", (key, body, result))
            database.commit()
        print(f"POST /builds: {key}; {'reused' if existing else 'created'} job", flush=True)
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(result.encode())

    def log_message(self, *_):
        pass


print("Local API fixture ready; POST /builds accepts an Idempotency-Key", flush=True)
HTTPServer(("127.0.0.1", int(os.environ.get("API_PORT", "9090"))), Handler).serve_forever()
