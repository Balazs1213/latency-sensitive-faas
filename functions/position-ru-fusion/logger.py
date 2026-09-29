import logging


def setup_logging(module_name: str) -> logging.Logger:
    logger = logging.getLogger(module_name)

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "[%(asctime)s.%(msecs)03d] %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    if not logger.hasHandlers():
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    return logger
