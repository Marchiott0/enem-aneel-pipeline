"""Configuração centralizada de logs com rastreabilidade."""
import logging
import sys

def get_logger(name: str = "pipeline") -> logging.Logger:
    """Retorna uma instância configurada do logger com formatação padronizada."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            fmt="[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger
