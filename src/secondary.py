import os
import time
from flask import Flask, request
from logger import setup_logger

app = Flask(__name__)
logger = setup_logger("Secondary")

messages = []
max_seq = 0

DELAY_SECONDS = int(os.getenv("DELAY_SECONDS", "0"))


@app.route("/messages", methods=["GET"])
def get_messages():
    return {"messages": messages}


@app.route("/replicate", methods=["POST"])
def replicate_message():
    global max_seq

    message_obj = request.json
    seq = message_obj["seq"]
    message = message_obj["message"]

    logger.info(f"Received: seq={seq} msg={message}")

    if seq <= max_seq:
        logger.info(f"Duplicate seq={seq}, ignoring")
        return {"status": "ack"}, 200

    messages.append(message_obj)
    max_seq = seq

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
