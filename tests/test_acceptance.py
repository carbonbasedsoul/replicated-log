#!/usr/bin/env python3
"""
Acceptance test from task requirements:
- Start M + S1
- send (Msg1, W=1) - Ok
- send (Msg2, W=2) - Ok
- send (Msg3, W=3) - Wait
- send (Msg4, W=1) - Ok
- Start S2
- Check messages on S2 - [Msg1, Msg2, Msg3, Msg4]
"""

import requests
import subprocess
import threading
import time
import sys

MASTER = "http://localhost:5000"
SECONDARY_PORT = 5002


def wait_for_master(timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            requests.get(f"{MASTER}/messages", timeout=5)
            return
        except requests.RequestException:
            time.sleep(1)
    raise RuntimeError("master not ready")


def post_message(msg, w, timeout=10):
    response = requests.post(
        f"{MASTER}/messages", json={"message": msg, "w": w}, timeout=timeout
    )
    response.raise_for_status()


def get_messages(port):
    response = requests.get(f"http://localhost:{port}/messages", timeout=5)
    response.raise_for_status()
    return response.json()["messages"]


def wait_for_messages(port, expected, timeout=20):
    deadline = time.time() + timeout
    last_payload = []
    while time.time() < deadline:
        try:
            last_payload = [m["message"] for m in get_messages(port)]
            if last_payload == expected:
                return True, last_payload
        except requests.RequestException:
            last_payload = []
        time.sleep(1)
    return False, last_payload


def run(cmd):
    return subprocess.run(cmd, check=True, capture_output=True)


def main():
    print("Acceptance Test")
    print("=" * 50)

    try:
        print("Setup: master + secondary-1")
        run(["docker-compose", "up", "-d", "--build"])
        wait_for_master()
        run(["docker-compose", "stop", "secondary-2"])
        time.sleep(3)

        print("Msg1 (w=1)")
        post_message("Msg1", 1)

        print("Msg2 (w=2)")
        post_message("Msg2", 2)

        print("Msg3 (w=3) – expect block")
        msg3_done = threading.Event()
        msg3_error = {}

        def send_msg3():
            try:
                post_message("Msg3", 3, timeout=60)
                msg3_done.set()
            except Exception as exc:  # noqa: BLE001
                msg3_error["message"] = str(exc)

        msg3_thread = threading.Thread(target=send_msg3, daemon=True)
        msg3_thread.start()
        time.sleep(2)

        print("Msg4 (w=1) – should be fast")
        start = time.time()
        post_message("Msg4", 1)
        duration = time.time() - start
        if duration >= 2:
            print(f"✗ Msg4 took {duration:.2f}s (blocked)")
            return 1
        print(f"  ✓ completed in {duration:.2f}s")

        print("Starting secondary-2")
        run(["docker-compose", "start", "secondary-2"])

        print("Waiting for Msg3 to finish")
        msg3_thread.join(timeout=30)
        if not msg3_done.is_set():
            print(f"✗ Msg3 did not complete: {msg3_error.get('message', 'timeout')}")
            return 1

        print("Waiting for secondary-2 catch-up")
        expected = ["Msg1", "Msg2", "Msg3", "Msg4"]
        ok, payload = wait_for_messages(SECONDARY_PORT, expected, timeout=25)
        if not ok:
            print(f"✗ Expected {expected}, got {payload}")
            return 1
        print(f"  ✓ secondary-2 messages: {payload}")

        print("\n✓ ACCEPTANCE TEST PASSED")
        return 0

    except Exception as exc:  # noqa: BLE001
        print(f"\n✗ TEST FAILED: {exc}")
        return 1
    finally:
        subprocess.run(["docker-compose", "down"], capture_output=True)


if __name__ == "__main__":
    sys.exit(main())
