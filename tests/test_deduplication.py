#!/usr/bin/env python3
"""
Test deduplication with direct injection.
- Send message through master
- Inject duplicate directly to secondary
- Verify message appears exactly once
"""

import requests
import subprocess
import time
import sys

MASTER = "http://localhost:5000"


def wait_for_master(timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            requests.get(f"{MASTER}/messages", timeout=5)
            return
        except requests.RequestException:
            time.sleep(1)
    raise RuntimeError("master not ready")


def main():
    print("Deduplication Test")
    print("=" * 50)

    # Setup
    print("Starting containers...")
    subprocess.run(
        ["docker-compose", "up", "-d", "--build"], check=True, capture_output=True
    )
    wait_for_master()

    # Send msg1 through master
    print("Sending msg1 through master (w=2)...")
    response = requests.post(
        f"{MASTER}/messages", json={"message": "test-dedup", "w": 2}, timeout=5
    )
    response.raise_for_status()
    time.sleep(1)

    # Inject duplicate directly to secondary-1
    print("Injecting duplicate to secondary-1...")
    response = requests.post(
        "http://localhost:5001/replicate", json={"id": 1, "message": "test-dedup"}
    )
    assert response.status_code == 200

    # Check logs for duplicate detection
    print("Checking logs for duplicate detection...")
    result = subprocess.run(
        ["docker-compose", "logs", "secondary-1"], capture_output=True, text=True
    )

    if "Duplicate" in result.stdout:
        print("  ✓ Duplicate detected in logs")
    else:
        print("  ✗ No duplicate detection in logs")
        return 1

    # Verify exactly one message on secondary-1
    print("Verifying message count...")
    response = requests.get("http://localhost:5001/messages")
    messages = response.json()["messages"]

    if len(messages) == 1 and messages[0]["message"] == "test-dedup":
        print(f"  ✓ Exactly 1 message on secondary-1")
        print("\n✓ DEDUPLICATION TEST PASSED")
        return 0
    else:
        print(f"  ✗ Expected 1 message, got {len(messages)}")
        print("\n✗ DEDUPLICATION TEST FAILED")
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\n✗ TEST FAILED: {e}")
        sys.exit(1)
    finally:
        subprocess.run(["docker-compose", "down"], capture_output=True)
