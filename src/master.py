import os
import time
import requests
from flask import Flask, request
from logger import setup_logger

app = Flask(__name__)
logger = setup_logger("Master")

messages = []

# comma-separated list of Secondary URLs
SECONDARY_URLS = os.getenv("SECONDARY_URLS").split(",")


def replicate_to_secondaries(message):
    """Send message to all Secondaries and wait for ACKs (blocking)."""
    for secondary_url in SECONDARY_URLS:
        logger.info(f"Replicating to {secondary_url}")
        requests.post(f"{secondary_url}/replicate", json={"message": message})
        logger.info(f"ACK from {secondary_url}")


@app.route("/messages", methods=["POST"])
def append_message():
    start_time = time.time()
    message = request.json["message"]

    logger.info(f"Received: {message}")

    # store the message and replicate to secondaries
    messages.append(message)
    replicate_to_secondaries(message)

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
