import json

path_config = "./config/config.json"
def get_config():
    with open(file=path_config) as fd_config:
        config = json.load(fd_config)
        fd_config.close()
        return config