import logging


def setup_logger(name: str):
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s - %(asctime)s - %(name)s - %(message)s",
    )
    return logging.getLogger(name)
