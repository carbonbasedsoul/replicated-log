import os
import time
import requests
from flask import Flask, request
from logger import setup_logger

app = Flask(__name__)
logger = setup_logger("Master")

messages = []
sequence_counter = 0

SECONDARY_URLS = os.getenv("SECONDARY_URLS").split(",")


def replicate_to_secondaries(message_obj):
    """Send message to all Secondaries and wait for ACKs (blocking)."""
    for secondary_url in SECONDARY_URLS:
        logger.info(f"Replicating to {secondary_url}")
        requests.post(f"{secondary_url}/replicate", json=message_obj)
        logger.info(f"ACK from {secondary_url}")


@app.route("/messages", methods=["POST"])
def append_message():
    global sequence_counter

    start_time = time.time()
    message = request.json["message"]

    sequence_counter += 1
    message_obj = {"seq": sequence_counter, "message": message}

    logger.info(f"Received: seq={sequence_counter} msg={message}")

    messages.append(message_obj)
    replicate_to_secondaries(message_obj)

    elapsed = time.time() - start_time
    logger.info(f"Message stored and replicated, duration: {int(elapsed)}s")

    return {"status": "success"}, 201


@app.route("/messages", methods=["GET"])
def get_messages():
    return {"messages": messages}


if __name__ == "__main__":
    logger.info("Master starting on port 5000")
    logger.info(f"Secondaries: {SECONDARY_URLS}")
    app.run(host="0.0.0.0", port=5000)
