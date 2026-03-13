import logging
from pathlib import Path

from .utils import LOG_DIR

info_handler = None
error_handler = None
console_handler = None
logger = None

# Custom formatter
class CustomFormatter(logging.Formatter):
    def format(self, record):
        # Extract the module/file name without extension
        module_name = record.module
        # Get function name
        func_name = record.funcName
        # Get time in HH:MM:SS format
        time_str = self.formatTime(record, "%H:%M:%S")

        # Custom format: $status:$time:$module_or_file:$function: message
        return f"{record.levelname}:{time_str}:{module_name}:{func_name}: {record.getMessage()}"

formatter = CustomFormatter()

# Create a logger instance
logger = logging.getLogger("shared_logger")
logger.setLevel(logging.DEBUG)

# Handler for INFO logs
info_handler = logging.FileHandler(LOG_DIR / "info.log")
info_handler.setLevel(logging.INFO)
info_handler.setFormatter(formatter)
logger.addHandler(info_handler)

# Handler for ERROR logs
error_handler = logging.FileHandler(LOG_DIR / "error.log")
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(formatter)
logger.addHandler(error_handler)

    # Console handler (both INFO and ERROR to stdout)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
