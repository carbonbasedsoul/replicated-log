from flask import Flask, request
from logger import setup_logger

app = Flask(__name__)
logger = setup_logger("Secondary")

messages = []


@app.route("/messages", methods=["GET"])
def get_messages():
    logger.info(f"Fetching {len(messages)} messages")
    return {"messages": messages}


@app.route("/replicate", methods=["POST"])
def replicate_message():
    message = request.json.get("message")

    if not message:
        return {"error": "Message required"}, 400

    messages.append(message)
    logger.info(f"Replicated: {message}")

    return {"status": "ack"}, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
