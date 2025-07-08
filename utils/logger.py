import logging

from utils import config

def setup_logger(logger_name, log_file, level):
    logger = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s')
    fileHandler = logging.FileHandler(log_file, mode="a")
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)

    logger.setLevel(level)
    logger.addHandler(fileHandler)
    logger.addHandler(streamHandler)
    
path_log = config.get_config().get("logger", {}).get("path_log", "./data/log.txt")
setup_logger("logger", path_log, logging.DEBUG)