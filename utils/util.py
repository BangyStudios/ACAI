def get_key_value(dict, value):
    return next((k for k, v in dict.items() if v == value), None)