import os
import time
import random
from flask import Flask, request
from logger import setup_logger

app = Flask(__name__)
lg = setup_logger("Secondary")

messages = {}
max_id = 0



@app.route("/messages", methods=["GET"])
def get_messages():
    return {"messages": messages}


@app.route("/replicate", methods=["POST"])
def replicate_message():
    global max_id

    msg_obj = request.json
    msg_id = msg_obj["id"]



    # check for duplication
    if msg_id <= max_id:
        lg.info(f"[id={msg_id}] Duplicate")
        return {"status": "ack"}, 200

    # store the message
    messages[msg_id] = msg_obj
    max_id = msg_id

    lg.info("ACK sent")
    return {"status": "ack"}, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
