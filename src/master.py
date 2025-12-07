import os
import time
import requests
from flask import Flask, request
from logger import setup_logger
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)
logger = setup_logger("Master")

messages = []
sequence_counter = 0

SECONDARY_URLS = os.getenv("SECONDARY_URLS").split(",")


def send_to_secondary(secondary_url, message_obj):
    """Send message to one Secondary and return when ACK received."""
    logger.info(f"Replicating to {secondary_url}")
    requests.post(f"{secondary_url}/replicate", json=message_obj, timeout=10)
    logger.info(f"ACK from {secondary_url}")
    return


def replicate_to_secondaries(message_obj, w):
    """Send message to all Secondaries concurrently, wait for w ACKs."""
    if w == 1:  # w=1
        logger.info("w=1: master ACK, not waiting for secondaries")
        executor = ThreadPoolExecutor()
        for secondary_url in SECONDARY_URLS:
            executor.submit(send_to_secondary, secondary_url, message_obj)
        executor.shutdown(wait=False)  # Don't wait for threads to finish
        return

    # w > 1: wait for (w-1) secondary ACKs
    acks_needed = w - 1
    logger.info(f"w={w}: waiting for {acks_needed} secondary ACKs")

    executor = ThreadPoolExecutor()
    futures = [
        executor.submit(send_to_secondary, url, message_obj) for url in SECONDARY_URLS
    ]

    ack_count = 0
    for future in as_completed(futures):
        ack_count += 1
        if ack_count >= acks_needed:
            logger.info(f"Got {acks_needed} ACKs, responding to client")
            executor.shutdown(wait=False)  # Don't wait for remaining threads
            return


@app.route("/messages", methods=["POST"])
def append_message():
    global sequence_counter

    start_time = time.time()
    message = request.json["message"]
    w = request.json.get("w", 1)  # Write concern: default w=1

    sequence_counter += 1
    message_obj = {"seq": sequence_counter, "message": message}

    logger.info(f"Received: seq={sequence_counter} msg={message} w={w}")

    messages.append(message_obj)
    logger.info(f"Message #{sequence_counter} stored")
    replicate_to_secondaries(message_obj, w)

    elapsed = time.time() - start_time
    logger.info(f"Responding to client, {int(elapsed)}s elapsed")

    return {"status": "success"}, 201


@app.route("/messages", methods=["GET"])
def get_messages():
    return {"messages": messages}


if __name__ == "__main__":
    logger.info("Master starting on port 5000")
    logger.info(f"Secondaries: {SECONDARY_URLS}")
    app.run(host="0.0.0.0", port=5000)
