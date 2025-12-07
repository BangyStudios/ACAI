import logging
import requests

from utils import config 

class Solar:
    def __init__(self):
        self.logger = logging.getLogger("logger")
        self.config = config.get_config().get("api", {}).get("power", {})
    
    def get_consumption_net(self):
        try:
            r = requests.get(self.config.get("url_consumption_net"), timeout=self.config.get("timeout"))
            return float(r.text.strip())
        except Exception as e:
            self.logger.error(f"Error fetching consumption: {e}")
            return None