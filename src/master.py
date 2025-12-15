import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import requests
from flask import Flask, request

from logger import setup_logger

app = Flask(__name__)
logger = setup_logger("Master")

messages = []
msg_counter = 0

SECONDARY_URLS = os.getenv("SECONDARY_URLS").split(",")

sec_health = {
    url: {
        "status": "healthy",
        "last_seen": time.time(),
        "last_error": None,
        "backoff_interval": 1.0,
    }
    for url in SECONDARY_URLS
}


def send_to_secondary(url: str, msg_obj):
    """Send message to one Secondary with retry until ACK received."""
    msg_id = msg_obj["id"]
    attempt = 1

    while True:
        # log first and every 5th attempt
        log_attempt: bool = attempt == 1 or attempt % 5 == 0

        try:
            response = requests.post(f"{url}/replicate", json=msg_obj, timeout=5)
            if response.status_code == 200:
                # reset backoff on successful delivery
                sec_health[url]["backoff_interval"] = 1.0
                return
            elif log_attempt:
                logger.info(f"Retrying {url} with msg-{msg_id} (attempt {attempt})")
        except requests.exceptions.RequestException as e:
            if log_attempt:
                logger.info(f"{url}: {type(e).__name__} (attempt {attempt})")

        interval = sec_health[url]["backoff_interval"]
        time.sleep(interval)

        # increase backoff if suspected/unhealthy, cap at 30s
        if sec_health[url]["status"] in ["suspected", "unhealthy"]:
            sec_health[url]["backoff_interval"] = min(interval * 2, 30)

        attempt += 1


def replicate_to_secondaries(msg_obj, w):
    """Send message to all Secondaries concurrently, wait for w ACKs."""
    if w == 1:
        executor = ThreadPoolExecutor()
        for url in SECONDARY_URLS:
            executor.submit(send_to_secondary, url, msg_obj)
        executor.shutdown(wait=False)
        return

    acks_needed = w - 1
    executor = ThreadPoolExecutor()
    futures = [
        executor.submit(send_to_secondary, url, msg_obj) for url in SECONDARY_URLS
    ]

    ack_count = 0
    for future in as_completed(futures):
        ack_count += 1
        if ack_count >= acks_needed:
            executor.shutdown(wait=False)
            return


@app.route("/messages", methods=["POST"])
def append_message():
    global msg_counter

    msg = request.json["message"]
    w = request.json.get("w", 1)

    # count healthy nodes
    healthy_count = sum(
        1 for health_data in sec_health.values() if health_data["status"] == "healthy"
    )
    available = healthy_count + 1  # +1 for master itself

    # read-only mode if insufficient healthy nodes
    if w > available:
        logger.info(
            f"Read-only mode: w={w} > available={available} (healthy secondaries: {healthy_count})"
        )
        return {
            "error": "insufficient healthy replicas",
            "required": w,
            "available": available,
        }, 503

    msg_counter += 1
    msg_obj: dict[str, int | str] = {"id": msg_counter, "message": msg}
    msg_obj = {"id": msg_counter, "message": msg}

    logger.info(f"Received [id={msg_counter}, w={w}] msg: {msg}")

    messages.append(msg_obj)
    replicate_to_secondaries(msg_obj, w)
    logger.info("DONE: responding to client")

    return {"status": "success"}, 201


@app.route("/messages", methods=["GET"])
def get_messages():
    return {"messages": messages}


@app.route("/catch-up", methods=["POST"])
def catch_up():
    max_id = request.json.get("max_id", 0)
    missing = [msg for msg in messages if msg["id"] > max_id]
    logger.info(f"Catch-up: sending {len(missing)} messages after id={max_id}")
    return {"messages": missing}


@app.route("/health", methods=["GET"])
def get_heartbeat():
    heartbeat_summary = []
    for url, health_data in sec_health.items():
        heartbeat = {
            "url": url,
            "status": health_data["status"],
            "last_seen": datetime.fromtimestamp(health_data["last_seen"]).isoformat(),
            "last_error": health_data["last_error"],
            "backoff_interval": health_data["backoff_interval"],
        }
        heartbeat_summary.append(heartbeat)

    return {"secondaries": heartbeat_summary}


def heartbeat_loop():
    while True:
        for url in SECONDARY_URLS:
            try:
                response = requests.get(f"{url}/health", timeout=2)
                if response.status_code == 200:
                    sec_health[url]["last_seen"] = time.time()
                    sec_health[url]["last_error"] = None
            except requests.exceptions.RequestException as e:
                sec_health[url]["last_error"] = f"{type(e).__name__}"

            elapsed = time.time() - sec_health[url]["last_seen"]
            old_status = sec_health[url]["status"]

            if elapsed < 6:
                new_status = "healthy"
            elif elapsed < 9:
                new_status = "suspected"
            else:
                new_status = "unhealthy"

            if new_status != old_status:
                logger.info(f"{url}: {old_status} → {new_status}")
                sec_health[url]["status"] = new_status

        time.sleep(3)


if __name__ == "__main__":
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=5000)
