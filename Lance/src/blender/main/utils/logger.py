import logging


def logger(
    name: str,
    level: int = logging.INFO,
    log_file: str = "logs/lance_codec.log",
) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # 避免重复添加 handler

    logger.setLevel(level)

    # 控制台 handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    fmt = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # 文件 handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(level)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


if __name__ == "__main__":
    logger = logger("test")
