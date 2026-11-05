import logging
import time

import requests

from utils import config


class Solar:
    def __init__(self):
        self.logger = logging.getLogger("logger")
        self.config = config.get_config().get("api", {}).get("power", {})
    
    def get_consumption_net(self, n_retry=3):
        for attempt in range(n_retry):
            try:
                r = requests.get(self.config.get("url_consumption_net"), timeout=self.config.get("timeout"))
                return float(r.text.strip())
            except Exception as e:
                if attempt < n_retry:
                    self.logger.warning(f"Error fetching consumption (attempt {attempt + 1}/{n_retry + 1}): {e}; retrying")
                    time.sleep(1)
                else:
                    self.logger.error(f"Error fetching consumption after {n_retry + 1} attempts: {e}")
        return None