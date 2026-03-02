"""
S2 log streaming client.
Buffers log lines and appends them to an S2 stream in batches.
Falls back to print() if S2 is not configured.
"""

import json
import os
import threading
import time
import urllib.parse
import urllib.request


class S2Logger:
    """Streams logs and progress to an S2 stream."""

    def __init__(self, stream=None, basin=None, token=None):
        self.stream = stream or os.environ.get("S2_STREAM")
        self.basin = basin or os.environ.get("S2_BASIN", "langelic")
        self.token = token or os.environ.get("S2_ACCESS_TOKEN")
        self.enabled = bool(self.stream and self.token)

        self._buffer = []
        self._lock = threading.Lock()
        self._last_flush = time.time()
        self._flush_interval = 2.0  # seconds
        self._flush_size = 50  # records
        self._stopped = False

        if self.enabled:
            self._base_url = f"https://{self.basin}.b.aws.s2.dev/v1"
            self._encoded_stream = urllib.parse.quote(self.stream, safe="")
            self._flush_thread = threading.Thread(target=self._auto_flush_loop, daemon=True)
            self._flush_thread.start()
            print(f"[S2Logger] Streaming to s2://{self.basin}/{self.stream}")
        else:
            print("[S2Logger] S2 not configured, logging to stdout only")

    def log(self, message):
        """Buffer a log line. Also prints to stdout."""
        print(message)
        if not self.enabled:
            return

        record = {
            "headers": [["type", "log"]],
            "body": message,
        }

        with self._lock:
            self._buffer.append(record)
            should_flush = len(self._buffer) >= self._flush_size

        if should_flush:
            self._flush()

    def progress(self, stats):
        """Send a progress record immediately. Also prints summary."""
        summary = f"[progress] time={stats.get('time', '?')} speed={stats.get('speed', '?')} fps={stats.get('fps', '?')}"
        print(summary)
        if not self.enabled:
            return

        record = {
            "headers": [["type", "progress"]],
            "body": json.dumps(stats),
        }

        with self._lock:
            self._buffer.append(record)

        # Flush immediately for progress records
        self._flush()

    def close(self):
        """Flush remaining buffer and send a done record."""
        self._stopped = True
        if not self.enabled:
            return

        done_record = {
            "headers": [["type", "done"]],
            "body": "encoding complete",
        }

        with self._lock:
            self._buffer.append(done_record)

        self._flush()
        print("[S2Logger] Stream closed")

    def _auto_flush_loop(self):
        """Background thread that flushes the buffer periodically."""
        while not self._stopped:
            time.sleep(self._flush_interval)
            with self._lock:
                should_flush = bool(self._buffer) and (time.time() - self._last_flush) >= self._flush_interval

            if should_flush:
                self._flush()

    def _flush(self):
        """Drain buffered records and send to S2. Lock is held only to copy+clear the buffer."""
        with self._lock:
            if not self._buffer:
                return
            records = self._buffer[:]
            self._buffer.clear()
            self._last_flush = time.time()

        # HTTP call runs outside the lock so log() isn't blocked
        try:
            self._append(records)
        except Exception as e:
            print(f"[S2Logger] Flush failed: {e}")

    def _append(self, records):
        """POST records to S2 append endpoint."""
        url = f"{self._base_url}/streams/{self._encoded_stream}/records"
        body = json.dumps({"records": records}).encode("utf-8")

        req = urllib.request.Request(
            url,
            data=body,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status != 200:
                    print(f"[S2Logger] Append returned {resp.status}")
        except Exception as e:
            print(f"[S2Logger] Append error: {e}")
