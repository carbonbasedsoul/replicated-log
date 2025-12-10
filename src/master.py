import os
import time
import requests
from flask import Flask, request
from logger import setup_logger
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)
lg = setup_logger("Master")

messages = []
msg_counter = 0

SECONDARY_URLS = os.getenv("SECONDARY_URLS").split(",")


def send_to_secondary(url, msg_obj):
    """Send message to one Secondary with retry until ACK received."""
    msg_id = msg_obj["id"]
    attempt = 1

    while True:
        log_attempt: bool = attempt == 1 or attempt % 5 == 0

        try:
            response = requests.post(f"{url}/replicate", json=msg_obj, timeout=5)
            if response.status_code == 200:
                return
            elif log_attempt:
                lg.info(f"Retrying {url} with msg-{msg_id} (attempt {attempt})")
        except requests.exceptions.RequestException as e:
            if log_attempt:
                lg.info(f"{url}: {type(e).__name__} (attempt {attempt})")

        time.sleep(1)
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

    msg_counter += 1
    msg_obj = {"id": msg_counter, "message": msg}

    lg.info(f"Received [id={msg_counter}, w={w}] msg: {msg}")

    messages.append(msg_obj)
    replicate_to_secondaries(msg_obj, w)
    lg.info("DONE: responding to client")

    return {"status": "success"}, 201


@app.route("/messages", methods=["GET"])
def get_messages():
    return {"messages": messages}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
