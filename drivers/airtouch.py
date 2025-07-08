from datetime import datetime
from lib.airtouch4pyapi.airtouch4pyapi.airtouch import AirTouch
from utils.util import get_key_value

fieldmap_airtouch_ac = {
    "AcFanSpeed": {
        0: "Auto", 
        1: "Low", 
        2: "Medium", 
        3: "High"
    }, 
    "AcMode": {
        0: "Auto", 
        1: "Cool", 
        2: "Dry", 
        3: "Fan", 
        4: "Heat" 
    }, 
    "IsOn": {
        False: "Off", 
        True: "On"
    }, 
    "PowerState": {
        False: "Off", 
        True: "On"
    }, 
    "Spill": {
        False: "Inactive", 
        True: "Active"
    }
}

fieldmap_airtouch_group = {
    "ControlMethod": {
        0: "PercentageControl", 
        1: "TemperatureControl"
    }, 
    "IsOn": {
        False: "Off", 
        True: "On"
    }, 
    "PowerState": {
        False: "Off", 
        True: "On"
    }, 
    "Spill": {
        False: "Inactive", 
        True: "Active"
    }
}
            
class AirTouchAC:
    def __init__(self, iot_ip):
        self.api = AirTouch(ipAddress=iot_ip)
        
    async def get_info(self):
        await self.api.UpdateInfo()
        return {
            "acs": self.api.GetAcs(), 
            "groups": self.api.GetGroups()
        }
        
class AirTouchDB:       
    def __init__(self, db):
        self.db = db
    
    async def ensure_tables(self):
        await self.db.execute_command("""
            CREATE TABLE IF NOT EXISTS airtouch_ac (
                datetime DATETIME NOT NULL, 
                AcFanSpeed INT, 
                AcMode INT, 
                AcNumber INT NOT NULL, 
                AcTargetSetpoint INT, 
                IsOn BOOLEAN, 
                PowerState BOOLEAN, 
                Spill BOOLEAN, 
                Temperature FLOAT, 
                PRIMARY KEY (DateTime, AcNumber)
            );
        """)

        await self.db.execute_command("""
            CREATE TABLE IF NOT EXISTS airtouch_group (
                datetime DATETIME NOT NULL, 
                ControlMethod INT, 
                GroupNumber INT NOT NULL, 
                IsOn BOOLEAN, 
                OpenPercent INT, 
                PowerState BOOLEAN, 
                Spill BOOLEAN, 
                TargetSetpoint INT, 
                Temperature FLOAT, 
                PRIMARY KEY (DateTime, GroupNumber)
            );
        """)
        
    async def save_info(self, ac_info):
        datetime_now = datetime.now()
        
        acs = ac_info["acs"]
        groups = ac_info["groups"]
        
        for ac in acs:
            await self.db.execute_command(
                """
                REPLACE INTO airtouch_ac (
                    datetime, 
                    AcFanSpeed, 
                    AcMode, 
                    AcNumber, 
                    AcTargetSetpoint, 
                    IsOn, 
                    PowerState, 
                    Spill, 
                    Temperature
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                params=(
                    datetime_now, 
                    get_key_value(fieldmap_airtouch_ac.get("AcFanSpeed"), ac.AcFanSpeed), 
                    get_key_value(fieldmap_airtouch_ac.get("AcMode"), ac.AcMode), 
                    ac.AcNumber, 
                    ac.AcTargetSetpoint, 
                    get_key_value(fieldmap_airtouch_ac.get("IsOn"), ac.IsOn), 
                    get_key_value(fieldmap_airtouch_ac.get("PowerState"), ac.PowerState), 
                    get_key_value(fieldmap_airtouch_ac.get("Spill"), ac.Spill), 
                    ac.Temperature
                )
            )
        
        for group in groups:
            await self.db.execute_command(
                """
                REPLACE INTO airtouch_group (
                    datetime, 
                    ControlMethod, 
                    GroupNumber, 
                    IsOn, 
                    OpenPercent, 
                    PowerState, 
                    Spill, 
                    TargetSetpoint, 
                    Temperature
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                params=(
                    datetime_now, 
                    get_key_value(fieldmap_airtouch_group.get("ControlMethod"), group.ControlMethod), 
                    group.GroupNumber,
                    get_key_value(fieldmap_airtouch_group.get("IsOn"), group.IsOn), 
                    group.OpenPercent, 
                    get_key_value(fieldmap_airtouch_group.get("PowerState"), group.PowerState), 
                    get_key_value(fieldmap_airtouch_group.get("Spill"), group.Spill),                     
                    group.TargetSetpoint, 
                    group.Temperature
                )
            )