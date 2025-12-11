import os
import time
import random
import threading
import requests
from flask import Flask, request
from logger import setup_logger

app = Flask(__name__)
lg = setup_logger("Secondary")

messages = {}
max_id = 0
max_contiguous_id = 0  # id of the last message still in total order in this secondary

MASTER_URL = os.getenv("MASTER_URL", "http://master:5000")


def store_message(msg_obj):
    """Store message with deduplication and total order enforcement."""
    global max_id, max_contiguous_id

    msg_id = msg_obj["id"]

    if msg_id in messages:
        return False  # duplicate

    messages[msg_id] = msg_obj
    if msg_id > max_id:
        max_id = msg_id

    while max_contiguous_id + 1 in messages:
        max_contiguous_id += 1

    return True  # successfully stored


@app.route("/messages", methods=["GET"])
def get_messages():
    contiguous_messages = [
        messages[msg_id] for msg_id in range(1, max_contiguous_id + 1)
    ]
    return {"messages": contiguous_messages}


@app.route("/replicate", methods=["POST"])
def replicate_message():
    msg_obj = request.json

    if store_message(msg_obj):
        lg.info("ACK sent")
    else:
        lg.info(f"[id={msg_obj['id']}] Duplicate, skipping")

    return {"status": "ack"}, 200


def catch_up_from_master():
    """Request missing messages from master on startup."""
    current_max_id = max_id
    time.sleep(2)

    response = requests.post(f"{MASTER_URL}/catch-up", json={"max_id": current_max_id})
    missing = response.json()["messages"]

    if missing:
        for msg_obj in missing:
            store_message(msg_obj)
        lg.info(f"Caught up: {len(missing)} messages")


if __name__ == "__main__":
    threading.Thread(target=catch_up_from_master, daemon=True).start()
    app.run(host="0.0.0.0", port=5001)
