#!/usr/bin/env python3
"""
Test catch-up replication after secondary restart.
Steps:
- Send two messages with both secondaries online
- Stop secondary-2 (simulate outage)
- Send two more messages while it is down
- Restart secondary-2 and wait for catch-up
- Verify log shows catch-up and node has all messages
"""

import requests
import subprocess
import time
import sys

MASTER = "http://localhost:5000"
SECONDARY_PORT = 5002


def send_message(message: str, w: int = 2):
    response = requests.post(
        f"{MASTER}/messages",
        json={"message": message, "w": w},
        timeout=5,
    )
    response.raise_for_status()


def get_secondary_messages():
    response = requests.get(f"http://localhost:{SECONDARY_PORT}/messages", timeout=5)
    response.raise_for_status()
    return response.json()["messages"]


def run(cmd):
    return subprocess.run(cmd, check=True, capture_output=True, text=True)


def main():
    print("Catch-up Test")
    print("=" * 50)

    try:
        print("Starting cluster...")
        run(["docker-compose", "up", "-d", "--build"])
        time.sleep(3)

        print("Sending initial messages (msg1, msg2)...")
        send_message("msg1")
        send_message("msg2")

        print("Stopping secondary-2...")
        run(["docker-compose", "stop", "secondary-2"])
        time.sleep(1)

        print("Sending messages while secondary-2 is down (msg3, msg4)...")
        send_message("msg3")
        send_message("msg4")

        print("Restarting secondary-2...")
        run(["docker-compose", "start", "secondary-2"])
        time.sleep(5)  # allow catch-up thread to run

        logs = run(["docker-compose", "logs", "secondary-2"]).stdout
        catch_up_lines = [line for line in logs.splitlines() if "Caught up:" in line]

        if not catch_up_lines:
            print("✗ No catch-up log found for secondary-2")
            return 1

        print(f"  ✓ {catch_up_lines[-1].strip()}")

        messages = get_secondary_messages()
        payloads = [msg["message"] for msg in messages]

        expected = ["msg1", "msg2", "msg3", "msg4"]
        if payloads == expected:
            print(f"  ✓ secondary-2 has messages: {payloads}")
            print("\n✓ CATCH-UP TEST PASSED")
            return 0

        print(f"✗ Expected {expected}, got {payloads}")
        return 1

    except Exception as exc:  # noqa: BLE001
        print(f"✗ TEST FAILED: {exc}")
        return 1
    finally:
        subprocess.run(["docker-compose", "down"], capture_output=True)


if __name__ == "__main__":
    sys.exit(main())
