import math

def get_key_value(dict, value):
    return next((k for k, v in dict.items() if v == value), None)

def sigmoid(x, radius=1, incline=10, decay=True):
    result = (0 if decay else 1) - math.exp(-((x/(radius/2)))**incline)
    return result

def linear_ramp(x, x_min, x_max):
    """
    Maps x to y in [0,1] linearly between x_min and x_max.
    
    - Normal case (x_min < x_max):
        x <= x_min -> 0
        x >= x_max -> 1
        otherwise linear interpolation between 0 and 1
        
    - Reversed case (x_min > x_max):
        x <= x_max -> 1
        x >= x_min -> 0
        otherwise linearly decreasing
        
    - Degenerate case (x_min == x_max):
        acts as a step at that value: y = 0 below, 1 at or above
    """
    
    if x_min == x_max:
        return 1.0 if x >= x_min else 0.0
    
    if x_min < x_max:
        if x <= x_min:
            return 0.0
        if x >= x_max:
            return 1.0

    else:
        if x <= x_max:
            return 1.0
        if x >= x_min:
            return 0.0

    return float((x - x_min) / (x_max - x_min))
