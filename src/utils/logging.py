import logging
import sys
import json
import time

def setup_logger(name="wildfire_logger", level=logging.INFO):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(level)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter(
            fmt="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

class StructuredLog:
    def __init__(self, component, step="EXECUTION"):
        self.component = component
        self.step = step
        self.start_time = time.time()
        self.metrics = {}

    def set_metric(self, key, value):
        self.metrics[key] = value

    def to_dict(self, status="SUCCESS", error=None):
        duration = round(time.time() - self.start_time, 2)
        d = {
            "component": self.component,
            "step": self.step,
            "duration_sec": duration,
            "status": status,
            **self.metrics
        }
        if error:
            d["error"] = str(error)
        return d

    def log(self, logger, status="SUCCESS", error=None):
        data = self.to_dict(status, error)
        msg = f"{self.component} :: {self.step} => Status={status} | Metrics={json.dumps(self.metrics)}"
        if error:
            msg += f" | Error={error}"
        if status == "SUCCESS":
            logger.info(msg)
        else:
            logger.error(msg)
