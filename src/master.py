from flask import Flask, request
from logger import setup_logger

app = Flask(__name__)
logger = setup_logger("Master")

messages = []


@app.route("/messages", methods=["POST"])
def append_message():
    message = request.json.get("message")

    if not message:
        return {"error": "Message required"}, 400

    messages.append(message)
    logger.info(f"Added: {message}")

    return {"status": "success"}, 201


@app.route("/messages", methods=["GET"])
def get_messages():
    logger.info(f"Fetching {len(messages)} messages")
    return {"messages": messages}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
