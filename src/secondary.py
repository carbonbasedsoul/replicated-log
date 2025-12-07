import os
import time
from flask import Flask, request
from logger import setup_logger

app = Flask(__name__)
logger = setup_logger("Secondary")

messages = []

DELAY_SECONDS = int(os.getenv("DELAY_SECONDS", "0"))


@app.route("/messages", methods=["GET"])
def get_messages():
    return {"messages": messages}


@app.route("/replicate", methods=["POST"])
def replicate_message():
    message = request.json["message"]

    logger.info(f"Received: {message}")
    messages.append(message)

    if DELAY_SECONDS > 0:
        logger.info(f"Sleeping {DELAY_SECONDS}s...")
        time.sleep(DELAY_SECONDS)

    logger.info("Sending ACK")
    return {"status": "ack"}, 200


if __name__ == "__main__":
    logger.info("Secondary starting on port 5001")
    if DELAY_SECONDS > 0:
        logger.info(f"Delay configured: {DELAY_SECONDS}s")
    app.run(host="0.0.0.0", port=5001)
