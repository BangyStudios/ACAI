from lib.airtouch4pyapi.airtouch4pyapi.airtouch import AirTouch, AirTouchVersion
import asyncio

class Controller:
    def __init__(self, host):
        """Constructor for the Air Conditioner Controller object"""
        self.host = host
        self.api = AirTouch(host)

    async def get_info(self):
        """Update information using API"""
        await self.api.UpdateInfo()

    async def get_temperature(self) -> None:
        """Returns the current temperature(s) for room(s)"""
        #TODO Add API to return global temperature
        pass

    async def set_mode_ac(self, ac, mode):
        """Sets the AC mode i.e. ['Cool', 'Fan', 'Dry', 'Heat', 'Auto']"""
        await self.api.SetCoolingModeForAc(ac, mode)
        
    async def set_fan_ac(self, ac, fan):
        """Sets the AC fan i.e. ['Auto', 'Low', 'Medium', 'High']"""
        await self.api.SetFanSpeedForAc(ac, fan)
        
    async def set_power_ac(self, ac, power):
        """Toggles the AC power on=1 or off=0"""
        if (power):
            await self.api.TurnAcOn(ac)
        else:
            await self.api.TurnAcOff(ac)
            
controller = Controller("192.168.1.7")
asyncio.run(controller.get_info())
asyncio.run(controller.set_power_ac(1, 1))