import asyncio
import copy
from datetime import datetime, timedelta
import logging
from typing import List, Tuple

import pandas as pd
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
    def __init__(self, iot_ip, db_base):
        self.logger = logging.getLogger('logger')
        self.db_base = db_base
        
        self.api = AirTouch(ipAddress=iot_ip)
        self.db = AirTouchDB(db=self.db_base)
        self.info = None
        
    async def ensure_tables(self):
        await self.db.ensure_tables()
        
    async def get_info(self, update=False) -> dict:
        if self.info is None or update:
            await self.api.UpdateInfo()
            self.info = {
                "acs": self.api.GetAcs(), 
                "groups": self.api.GetGroups()
            }
            
        return self.info
    
    async def save_info(self):
        ac_info = await self.get_info(update=True)
        if self.info is not None:
            await self.db.save_info(ac_info=ac_info)
            self.logger.info("AC info successfully saved to DB")
        else:
            self.logger.error("Failed to save AC info to DB, ac_info is None")
    
    async def get_ac_ids(self) -> List[int]:
        return [int(ac.AcNumber) for ac in (await self.get_info()).get("acs", [])]
    
    async def get_ac_ids_on(self) -> List[int]:
        ac_ids_on = list()
        for ac in (await self.get_info()).get("acs", []):
            if ac.PowerState == "On":
                ac_ids_on.append(int(ac.AcNumber))
        return ac_ids_on

    async def get_group_ids_Sensor(self):
        group_ids_on = list()
        for group in (await self.get_info()).get("groups", []):
            if group.Sensor == "Yes":
                group_ids_on.append(int(group.GroupNumber))
        return group_ids_on

    async def get_mode_ac(self, ac_id) -> str:
        acs = (await self.get_info()).get("acs", [])
        if len(acs) > 0:
            for ac in acs:
                if ac.AcNumber == ac_id:
                    return ac.AcMode
            self.logger.error(f"Unable to find ac_id: {ac_id}, returning default: (16, 30)")
            return "Auto"
        else:
            self.logger.error("AC list empty, returning default: (16, 30)")
            return "Auto"

    async def get_range_T(self, ac_id) -> Tuple[int, int]:
        acs = (await self.get_info()).get("acs", [])
        if len(acs) > 0:
            for ac in acs:
                if ac.AcNumber == ac_id:
                    return int(ac.MinSetpoint), int(ac.MaxSetpoint)
            self.logger.error(f"Unable to find ac_id: {ac_id}, returning default: (16, 30)")
            return (16, 30)
        else:
            self.logger.error("AC list empty, returning default: (16, 30)")
            return (16, 30)
        
    async def get_T_ac_target(self, ac_id) -> int:
        acs = (await self.get_info(update=True)).get("acs", [])
        for ac in acs:
            if ac.AcNumber == ac_id:
                return int(ac.AcTargetSetpoint)
        self.logger.error(f"Unable to find ac_id: {ac_id}, returning default: 25")
        return 25
    
    async def get_T_ac_in(self, ac_id) -> float:
        acs = (await self.get_info(update=True)).get("acs", [])
        for ac in acs:
            if ac.AcNumber == ac_id:
                return int(ac.Temperature)
        self.logger.error(f"Unable to find ac_id: {ac_id}, returning default: 25")
        return 25
    
    async def get_T_groups(self, ac_id, require_sensor=True):
        T_groups = list()
        groups = (await self.get_info(update=True)).get("groups", [])
        for group in groups:
            if group.BelongsToAc == ac_id:
                if group.Sensor == "Yes" or not require_sensor:
                    T_groups.append(group.Temperature)
        return T_groups

    async def get_airflow_groups(self, ac_id, require_sensor=True):
        airflow_groups = list()
        groups = (await self.get_info(update=True)).get("groups", [])
        for group in groups:
            if group.BelongsToAc == ac_id:
                if group.Sensor == "Yes" or not require_sensor:
                    airflow_groups.append(group.OpenPercent / 100)
        return airflow_groups
    
    async def get_params_algorithm_reactive(self, ac_id):
        mode_ac = (await self.get_mode_ac(ac_id=ac_id)).lower()

        ac_range_T = await self.get_range_T(ac_id=ac_id)
        T_min = ac_range_T[0]
        T_max = ac_range_T[1]

        interval_history = 5

        # Get AC temperatures
        T_ac_target_current = await self.get_T_ac_target(ac_id=ac_id)
        T_ac_in_current = await self.get_T_ac_in(ac_id=ac_id)
        resampled_ac_last = await self.db.get_resampled_ac_last(ac_id=ac_id, n_last_mins=15, resample_mins=interval_history)
        T_ac_in_history = resampled_ac_last.get("Temperature", [])

        # Get group temperatures
        T_groups_current = await self.get_T_groups(ac_id=ac_id)
        group_ids_Sensor = await self.get_group_ids_Sensor()
        resampled_groups_last = await self.db.get_resampled_groups_last(ac_id=ac_id, n_last_mins=60, resample_mins=interval_history, group_ids=group_ids_Sensor)
        T_groups_history = list()
        for index_time in range(len(resampled_groups_last.get("datetime", []))):
            T_groups_history.append(list())
            for group in resampled_groups_last.get("groups", []):
                T_groups_history[index_time].append(group.get("data").get("Temperature")[index_time])

        # Get group airflows
        airflow_groups_current = await self.get_airflow_groups(ac_id)

        return (
            mode_ac,
            T_min,
            T_max,
            T_ac_target_current,
            T_ac_in_current,
            T_ac_in_history,
            T_groups_current,
            T_groups_history,
            interval_history,
            airflow_groups_current
        )

    async def set_T_ac_target(self, ac_id, T_ac_target):
        await self.api.SetTemperatureForAc(acNumber=ac_id, temperature=T_ac_target)

    async def set_airflow_groups(self, ac_id, airflow_groups):
        groups = (await self.get_info(update=True)).get("groups", [])
        index_group = 0
        for group in groups:
            if group.BelongsToAc == ac_id:
                if group.Sensor == "Yes":
                    await self.api.SetGroupToPercentage(group.GroupNumber, int(airflow_groups[index_group] * 100))
                index_group += 1
        return airflow_groups
        
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
                BelongsToAc INT, 
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
                    BelongsToAc, 
                    ControlMethod, 
                    GroupNumber, 
                    IsOn, 
                    OpenPercent, 
                    PowerState, 
                    Spill, 
                    TargetSetpoint, 
                    Temperature
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                params=(
                    datetime_now, 
                    group.BelongsToAc, 
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

    async def get_resampled_ac_last(
        self, 
        ac_id: int, 
        n_last_mins: int, 
        resample_mins: int
    ) -> dict:
        """
        Get resampled AC points for the given AC number.
        
        Args:
            ac_id: The AC unit number to query
            n_last_mins: Look back period in minutes
            resample_mins: Resampling interval in minutes
            
        Returns:
            dict: Dictionary of lists with resampled AC data
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=n_last_mins)
        
        ac_query = f"""
            SELECT 
                datetime,
                AcTargetSetpoint,
                Temperature
            FROM airtouch_ac
            WHERE 
                AcNumber = {ac_id}
                AND datetime BETWEEN '{start_time}' AND '{end_time}'
            ORDER BY datetime
        """
        
        ac_rows = await self.db.execute_query(ac_query)
        
        if not ac_rows:
            return {col: [] for col in [
                'datetime', 'AcTargetSetpoint', 'Temperature'
            ]}
        
        # Convert to DataFrame for easy resampling
        df = pd.DataFrame(ac_rows, columns=[
            'datetime', 'AcTargetSetpoint', 'Temperature'
        ])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        
        # Resample using mean for numeric, first for boolean/categorical
        resampled = df.resample(f'{resample_mins}min').agg({
            'AcTargetSetpoint': 'mean',
            'Temperature': 'mean'
        }).dropna(how='all')
        
        # Convert back to dict of lists
        result = {
            'datetime': resampled.index.to_pydatetime().tolist(),
            'AcTargetSetpoint': resampled['AcTargetSetpoint'].tolist(),
            'Temperature': resampled['Temperature'].tolist()
        }
        
        return result

    async def get_resampled_groups_last(
        self, 
        ac_id: int, 
        n_last_mins: int, 
        resample_mins: int,
        group_ids: List[int] = []
    ) -> dict:
        """
        Get resampled group points for an ac.
        
        Args:
            ac_id: The AC unit number to query
            n_last_mins: Look back period in minutes
            resample_mins: Resampling interval in minutes
            
        Returns:
            dict: Dictionary of lists with resampled group data
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=n_last_mins)

        group_filter = ""
        if group_ids:
            group_filter = f"AND GroupNumber IN ({','.join(map(str, group_ids))})"

        group_query = f"""
            SELECT 
                datetime,
                GroupNumber,
                OpenPercent,
                TargetSetpoint,
                Temperature
            FROM airtouch_group
            WHERE 
                BelongsToAc = {ac_id}
                AND datetime BETWEEN '{start_time}' AND '{end_time}'
                {group_filter}
            ORDER BY datetime, GroupNumber
        """
        
        group_rows = await self.db.execute_query(group_query)
        
        if not group_rows:
            return {col: [] for col in [
                'datetime', 'GroupNumber', 'OpenPercent', 'TargetSetpoint', 'Temperature'
            ]}
        
        # Convert to DataFrame for easy resampling
        df = pd.DataFrame(group_rows, columns=[
            'datetime', 'GroupNumber', 'OpenPercent', 'TargetSetpoint', 'Temperature'
        ])
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        # Resample each group separately
        groups = df['GroupNumber'].unique()
        result = {
            'datetime': [],
            'groups': []
        }
        
        for group in groups:
            group_df = df[df['GroupNumber'] == group].copy()
            group_df.set_index('datetime', inplace=True)
            
            resampled = group_df.resample(f'{resample_mins}min').agg({
                'GroupNumber': 'first',
                'OpenPercent': 'mean',
                'TargetSetpoint': 'mean',
                'Temperature': 'mean'
            }).dropna(how='all')
            
            if not resampled.empty:
                if not result['datetime']:
                    result['datetime'] = resampled.index.to_pydatetime().tolist()
                
                result['groups'].append({
                    'GroupNumber': group,
                    'data': {
                        'OpenPercent': resampled['OpenPercent'].tolist(),
                        'TargetSetpoint': resampled['TargetSetpoint'].tolist(),
                        'Temperature': resampled['Temperature'].tolist()
                    }
                })
        
        return result