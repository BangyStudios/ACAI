import legacy.controller as controller
controller = controller.Controller("192.168.0.14")

import time

ac_current = 1
time_ac_current = 0

while True:
    if (time_ac_current < 900 and ac_current == 0):
        controller.set_power_ac(0, 1)
        controller.set_power_ac(1, 0)
    if (time_ac_current >= 900 and ac_current == 0):
        ac_current = 1 # Change to AC1
        time_ac_current = 0 # Reset time for AC0
    if (time_ac_current < 900 and ac_current == 1):
        controller.set_power_ac(0, 0)
        controller.set_power_ac(1, 1)
    if (time_ac_current >= 900 and ac_current == 1):
        ac_current = 0 # Change to AC0
        time_ac_current = 0 # Reset time for AC1
    if (ac_current == 0):
            print(f"On AC0 {time_ac_current / 60}min(s)")
            time_ac_current += 30
    elif (ac_current == 1):
        print(f"On AC1 {time_ac_current / 60}min(s)")
        time_ac_current += 30
    time.sleep(30)