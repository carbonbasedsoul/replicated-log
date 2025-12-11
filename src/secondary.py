import os
import time
import random
from flask import Flask, request
from logger import setup_logger

app = Flask(__name__)
lg = setup_logger("Secondary")

messages = {}
max_id = 0
max_contiguous_id = 0  # id of the last message still in total order in this secondary


@app.route("/messages", methods=["GET"])
def get_messages():
    contiguous_messages = [
        messages[msg_id] for msg_id in range(1, max_contiguous_id + 1)
    ]
    return {"messages": contiguous_messages}


@app.route("/replicate", methods=["POST"])
def replicate_message():
    global max_id, max_contiguous_id

    msg_obj = request.json
    msg_id = msg_obj["id"]

    # check for duplication
    if msg_id <= max_id:
        lg.info(f"[id={msg_id}] Duplicate, skipping")
        return {"status": "ack"}, 200

    # store the message
    messages[msg_id] = msg_obj
    max_id = msg_id

    # update max_contiguous_id
    while max_contiguous_id + 1 in messages:
        max_contiguous_id += 1

    lg.info("ACK sent")
    return {"status": "ack"}, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
