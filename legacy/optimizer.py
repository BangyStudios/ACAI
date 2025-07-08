import legacy.controller as controller
import time
import datetime

controller = controller.Controller("192.168.0.14")
# mode_current = None
state_ac = True
ac_current = 1
time_ac_current = 0

while True:
    with open("/home/icb/Code/Enphase-Tools/data/temp.txt", "r") as solar_net:
        try:
            power_net = int(solar_net.read())
        except (ValueError):
            power_net = 0
        solar_net.close()
    if (power_net > 1000):
        state_ac = True
    elif (power_net < -5000):
        state_ac = False
    if (state_ac):
        controller.set_power_ac(0, 1)
        controller.set_power_ac(1, 1)
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
    elif (not state_ac):
        controller.set_power_ac(0, 0)
        controller.set_power_ac(1, 0)
        print(f"Off AC{ac_current} {time_ac_current / 60}min(s)")
    if (state_ac):
        if (ac_current == 0):
            print(f"On AC0 {time_ac_current / 60}min(s)")
            time_ac_current += 30
        elif (ac_current == 1):
            print(f"On AC1 {time_ac_current / 60}min(s)")
            time_ac_current += 30
    time.sleep(30)