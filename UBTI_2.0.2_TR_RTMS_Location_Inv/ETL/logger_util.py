import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import os
import sys
import warnings


def get_logger(etl_name):
    base_path = os.path.dirname(os.path.abspath(__file__))
    log_folder = os.path.join(base_path, "logs")
    os.makedirs(log_folder, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d")
    log_file = os.path.join(log_folder, f"{etl_name}_{timestamp}.log")

    # Create logger
    logger = logging.getLogger(etl_name)
    logger.setLevel(logging.DEBUG)     # allow debug logs if needed

    # Avoid duplicate handlers
    if not logger.handlers:
        handler = RotatingFileHandler(
            log_file,
            maxBytes=10_000_000,
            backupCount=5
        )

        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] [ETL:%(name)s] %(message)s"
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Also output to console (optional)
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        logger.addHandler(console)

        # --------- ERROR HANDLING ENHANCEMENTS ----------
        # 1) Write full exception if can not catch it
        def handle_uncaught_exceptions(exc_type, exc_value, exc_traceback):
            if logger:
                logger.critical(
                    "Uncaught exception occurred",
                    exc_info=(exc_type, exc_value, exc_traceback)
                )
            else:
                print("FATAL ERROR:", exc_value)

        sys.excepthook = handle_uncaught_exceptions

        # 2) Convert all Python warnings → logger.warning
        def warn_to_log(message, category, filename, lineno, file=None, line=None):
            logger.warning(f"{category.__name__}: {message} ({filename}:{lineno})")

        warnings.showwarning = warn_to_log
        # -------------------------------------------------

    return logger
