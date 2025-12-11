#!/usr/bin/env python3
"""
Test total order with direct injection.
- Send msg1, msg2, msg3 through master
- Inject msg5 directly to secondary (creating gap)
- Verify secondary hides msg5 until msg4 arrives
"""

import requests
import subprocess
import time
import sys


def get_messages(port):
    """Get visible messages from node."""
    response = requests.get(f"http://localhost:{port}/messages")
    return response.json()["messages"]


def main():
    print("Total Order Test")
    print("=" * 50)

    # Setup
    print("Starting containers...")
    subprocess.run(
        ["docker-compose", "up", "-d", "--build"], check=True, capture_output=True
    )
    time.sleep(3)

    # Send msg1, msg2, msg3 normally
    print("Sending msg1, msg2, msg3...")
    for i in range(1, 4):
        requests.post(
            "http://localhost:5000/messages", json={"message": f"msg{i}", "w": 2}
        )
    time.sleep(1)

    # Inject msg5 directly to secondary-2 (skip msg4)
    print("Injecting msg5 directly (creating gap at msg4)...")
    requests.post("http://localhost:5002/replicate", json={"id": 5, "message": "msg5"})
    time.sleep(1)

    # Check secondary-2 - should only show msg1, msg2, msg3 (hiding msg5)
    print("Checking secondary-2 hides msg5...")
    msgs = get_messages(5002)
    visible = [m["message"] for m in msgs]

    if visible == ["msg1", "msg2", "msg3"]:
        print(f"  ✓ Secondary-2 shows: {visible} (msg5 hidden)")
    else:
        print(f"  ✗ Expected [msg1, msg2, msg3], got {visible}")
        return 1

    # Send msg4 through master
    print("Sending msg4 (filling gap)...")
    requests.post("http://localhost:5000/messages", json={"message": "msg4", "w": 2})
    time.sleep(1)

    # Check secondary-2 - should now show all messages
    print("Checking secondary-2 now shows all messages...")
    msgs = get_messages(5002)
    visible = [m["message"] for m in msgs]

    if visible == ["msg1", "msg2", "msg3", "msg4", "msg5"]:
        print(f"  ✓ Secondary-2 shows: {visible} (gap filled)")
        print("\n✓ TOTAL ORDER TEST PASSED")
        return 0
    else:
        print(f"  ✗ Expected [msg1, msg2, msg3, msg4, msg5], got {visible}")
        print("\n✗ TOTAL ORDER TEST FAILED")
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\n✗ TEST FAILED: {e}")
        sys.exit(1)
    finally:
        subprocess.run(["docker-compose", "down"], capture_output=True)
