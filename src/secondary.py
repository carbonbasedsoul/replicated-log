from flask import Flask, request
from logger import setup_logger

app = Flask(__name__)
logger = setup_logger("Secondary")

messages = []


@app.route("/messages", methods=["GET"])
def get_messages():
    return {"messages": messages}


@app.route("/replicate", methods=["POST"])
def replicate_message():
    message = request.json["message"]

    logger.info(f"Received: {message}")
    messages.append(message)


    logger.info("Sending ACK")
    return {"status": "ack"}, 200


if __name__ == "__main__":
    logger.info("Secondary starting on port 5001")
    app.run(host="0.0.0.0", port=5001)
