import logging


def setup_logger(name: str):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(message)s",
        datefmt="%H:%M:%S",
    )

    # suppress flask HTTP request logs
    logging.getLogger("werkzeug").setLevel(logging.ERROR)

    return logging.getLogger(name)
