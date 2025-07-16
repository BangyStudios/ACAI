
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple, TypedDict

import pandas as pd
from lib.airtouch4pyapi.airtouch4pyapi.airtouch import AirTouch
from utils import config
from utils.util import get_key_value

# Define type aliases for better type hints
AcNumber = int
GroupNumber = int

# Define typed dictionaries for return values
class AcInfo(TypedDict):
    AcNumber: AcNumber
    AcFanSpeed: str
    AcMode: str
    IsOn: str
    PowerState: str
    Spill: str
    Temperature: float
    AcTargetSetpoint: int
    MinSetpoint: int
    MaxSetpoint: int

class GroupInfo(TypedDict):
    GroupNumber: GroupNumber
    BelongsToAc: AcNumber
    ControlMethod: str
    IsOn: str
    OpenPercent: int
    PowerState: str
    Spill: str
    TargetSetpoint: int
    Temperature: float
    Sensor: str

fieldmap_airtouch_ac = {
    "AcFanSpeed": {0: "Auto", 1: "Low", 2: "Medium", 3: "High"}, 
    "AcMode": {0: "Auto", 1: "Cool", 2: "Dry", 3: "Fan", 4: "Heat" }, 
    "IsOn": {False: "Off", True: "On"}, 
    "PowerState": {False: "Off", True: "On"}, 
    "Spill": {False: "Inactive", True: "Active"}
}

fieldmap_airtouch_group = {
    "ControlMethod": {0: "PercentageControl", 1: "TemperatureControl"}, 
    "IsOn": {False: "Off", True: "On"}, 
    "PowerState": {False: "Off", True: "On"}, 
    "Spill": {False: "Inactive", True: "Active"}
}
            
class AirTouchAC:
    def __init__(self, iot_ip: str, db_base):
        self.logger = logging.getLogger("logger")
        self.db_base = db_base
        self.config = config.get_config().get("drivers", {}).get("airtouch", {})
        
        self.api = AirTouch(ipAddress=iot_ip)
        self.db = AirTouchDB(db=self.db_base, config=self.config)
        self._info: Optional[Dict[str, List]] = None
        self._last_update: Optional[datetime] = None
        
    async def ensure_tables(self) -> None:
        """Ensure database tables exist."""
        await self.db.ensure_tables()
        
    async def get_info(self, update: bool = False) -> Dict[str, List]:
        """Get current AC system information.
        
        Args:
            update: Whether to force an update from the API
            
        Returns:
            Dictionary with 'acs' and 'groups' keys containing lists of ACs and groups
        """
        if self._info is None or update or self._should_update_cache():
            await self.api.UpdateInfo()
            self._info = {
                "acs": self.api.GetAcs(), 
                "groups": self.api.GetGroups()
            }
            self._last_update = datetime.now()
            
        return self._info
    
    def _should_update_cache(self) -> bool:
        """Determine if cache should be updated based on last update time."""
        if self._last_update is None:
            return True
        return (datetime.now() - self._last_update) > timedelta(minutes=self.config.get("cache_ttl_minutes"))
    
    async def save_info(self) -> None:
        """Save current AC information to database."""
        ac_info = await self.get_info()
        if ac_info is not None:
            await self.db.save_info(ac_info=ac_info)
            self.logger.info("AC info successfully saved to DB")
        else:
            self.logger.error("Failed to save AC info to DB, ac_info is None")
    
    async def get_ac_ids(self) -> List[AcNumber]:
        """Get list of all AC unit numbers."""
        return [int(ac.AcNumber) for ac in (await self.get_info()).get("acs", [])]
    
    async def get_ac_ids_on(self) -> List[AcNumber]:
        """Get list of AC unit numbers that are currently powered on."""
        return [
            int(ac.AcNumber) 
            for ac in (await self.get_info()).get("acs", []) 
            if ac.PowerState == "On"
        ]

    async def get_group_ids_with_sensor(self) -> List[GroupNumber]:
        """Get list of group numbers that have temperature sensors."""
        return [
            int(group.GroupNumber) 
            for group in (await self.get_info()).get("groups", []) 
            if group.Sensor == "Yes"
        ]

    async def get_ac_info(self, ac_id: AcNumber) -> Optional[AcInfo]:
        """Get complete information for a specific AC unit.
        
        Args:
            ac_id: The AC unit number to query
            
        Returns:
            Dictionary with AC information or None if not found
        """
        acs = (await self.get_info(update=True)).get("acs", [])
        for ac in acs:
            if ac.AcNumber == ac_id:
                return {
                    "AcNumber": ac_id,
                    "AcFanSpeed": ac.AcFanSpeed,
                    "AcMode": ac.AcMode, 
                    "IsOn": ac.IsOn, 
                    "PowerState": ac.PowerState,
                    "Spill": ac.Spill, 
                    "Temperature": float(ac.Temperature),
                    "AcTargetSetpoint": int(ac.AcTargetSetpoint),
                    "MinSetpoint": int(ac.MinSetpoint),
                    "MaxSetpoint": int(ac.MaxSetpoint)
                }
        self.logger.error(f"Unable to find ac_id: {ac_id}")
        return None

    async def get_mode_ac(self, ac_id: AcNumber) -> str:
        """Get the current mode of a specific AC unit."""
        ac_info = await self.get_ac_info(ac_id)
        return ac_info["AcMode"] if ac_info else "Auto"

    async def get_range_T(self, ac_id: AcNumber) -> Tuple[int, int]:
        """Get temperature range for a specific AC unit."""
        ac_info = await self.get_ac_info(ac_id)
        return (ac_info["MinSetpoint"], ac_info["MaxSetpoint"]) if ac_info else (self.config.get("defaults").get("temperature").get("min"), self.config.get("defaults").get("temperature").get("max"))
        
    async def get_T_ac_target(self, ac_id: AcNumber) -> int:
        """Get target temperature for a specific AC unit."""
        ac_info = await self.get_ac_info(ac_id)
        return ac_info["AcTargetSetpoint"] if ac_info else self.config.get("defaults").get("temperature").get("default")
    
    async def get_T_ac_in(self, ac_id: AcNumber) -> float:
        """Get current temperature for a specific AC unit."""
        ac_info = await self.get_ac_info(ac_id)
        return ac_info["Temperature"] if ac_info else None
    
    async def get_groups_info(self, ac_id: AcNumber, require_sensor: bool = True) -> List[GroupInfo]:
        """Get information for all groups belonging to a specific AC unit.
        
        Args:
            ac_id: The AC unit number to query
            require_sensor: Whether to only include groups with sensors
            
        Returns:
            List of dictionaries with group information
        """
        groups = (await self.get_info(update=True)).get("groups", [])
        return [
            {
                "GroupNumber": group.GroupNumber,
                "BelongsToAc": group.BelongsToAc,
                "ControlMethod": fieldmap_airtouch_group["ControlMethod"].get(group.ControlMethod, "Unknown"),
                "IsOn": fieldmap_airtouch_group["IsOn"].get(group.IsOn, "Unknown"),
                "OpenPercent": group.OpenPercent,
                "PowerState": fieldmap_airtouch_group["PowerState"].get(group.PowerState, "Unknown"),
                "Spill": fieldmap_airtouch_group["Spill"].get(group.Spill, "Unknown"),
                "TargetSetpoint": group.TargetSetpoint,
                "Temperature": group.Temperature,
                "Sensor": group.Sensor
            }
            for group in groups
            if group.BelongsToAc == ac_id and (not require_sensor or group.Sensor == "Yes")
        ]

    async def get_T_groups(self, ac_id: AcNumber, require_sensor: bool = True) -> Dict[GroupNumber, float]:
        """Get temperatures for groups belonging to a specific AC unit.
        
        Args:
            ac_id: The AC unit number to query
            require_sensor: Whether to only include groups with sensors
            
        Returns:
            Dictionary mapping group numbers to their temperatures
        """
        groups = await self.get_groups_info(ac_id, require_sensor)
        return {group["GroupNumber"]: group["Temperature"] for group in groups}

    async def get_airflow_groups(self, ac_id: AcNumber, require_sensor: bool = True) -> Dict[GroupNumber, float]:
        """Get airflow percentages for groups belonging to a specific AC unit.
        
        Args:
            ac_id: The AC unit number to query
            require_sensor: Whether to only include groups with sensors
            
        Returns:
            Dictionary mapping group numbers to their airflow percentages (0-1)
        """
        groups = await self.get_groups_info(ac_id, require_sensor)
        return {group["GroupNumber"]: group["OpenPercent"] / 100 for group in groups}
    
    async def get_params_algorithm_reactive(self, ac_id: AcNumber) -> dict:
        """Get all parameters needed for the reactive algorithm.
        
        Args:
            ac_id: The AC unit number to query
            
        Returns:
            Dictionary with all required parameters for the algorithm
        """        
        mode_ac = (await self.get_mode_ac(ac_id=ac_id)).lower()
        
        T_min, T_max = await self.get_range_T(ac_id=ac_id)

        # Get AC temperatures
        T_ac_target_current = await self.get_T_ac_target(ac_id=ac_id)
        T_ac_in_current = await self.get_T_ac_in(ac_id=ac_id)
        resampled_ac_last = await self.db.get_resampled_ac_last(
            ac_id=ac_id, 
            n_last_mins=self.config.get("history_minutes"), 
            resample_mins=self.config.get("resample_interval_minutes")
        )
        T_ac_in_history = resampled_ac_last.get("Temperature", [])

        # Get group temperatures
        T_groups_current = await self.get_T_groups(ac_id=ac_id)
        group_ids_Sensor = await self.get_group_ids_with_sensor()
        resampled_groups_last = await self.db.get_resampled_groups_last(
            ac_id=ac_id, 
            n_last_mins=self.config.get("history_minutes"), 
            resample_mins=self.config.get("resample_interval_minutes"), 
            group_ids=group_ids_Sensor
        )

        # Process group history data
        T_groups_history = []
        if resampled_groups_last.get("groups"):
            for index_time in range(len(resampled_groups_last.get("datetime", []))):
                T_groups_history.append([
                    group["data"]["Temperature"][index_time]
                    for group in resampled_groups_last["groups"]
                ])

        # Get group airflows
        airflow_groups_current = await self.get_airflow_groups(ac_id)

        return {
            "mode_ac": mode_ac,
            "T_min": T_min,
            "T_max": T_max,
            "T_ac_target_current": T_ac_target_current,
            "T_ac_in_current": T_ac_in_current,
            "T_ac_in_history": T_ac_in_history,
            "T_groups_current": T_groups_current.values(),
            "T_groups_history": T_groups_history,
            "interval_history": self.config.get("resample_interval_minutes"),
            "airflow_groups_current": airflow_groups_current.values()
        }

    async def set_T_ac_target(self, ac_id: AcNumber, T_ac_target: int) -> None:
        """Set target temperature for a specific AC unit."""
        await self.api.SetTemperatureForAc(acNumber=ac_id, temperature=T_ac_target)

    async def set_airflow_groups(self, ac_id: AcNumber, airflow_groups: Dict[GroupNumber, float]) -> None:
        """Set airflow percentages for groups belonging to a specific AC unit.
        
        Args:
            ac_id: The AC unit number that owns the groups
            airflow_groups: Dictionary mapping group numbers to airflow percentages (0-1)
        """
        groups_info = await self.get_groups_info(ac_id, require_sensor=True)
        
        for group in groups_info:
            group_number = group["GroupNumber"]
            if group_number in airflow_groups:
                percentage = int(airflow_groups[group_number] * 100)
                await self.api.SetGroupToPercentage(group_number, percentage)

class AirTouchDB:       
    def __init__(self, db, config):
        self.db = db
        self.config = config
        
    async def ensure_tables(self) -> None:
        """Ensure database tables exist."""
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
        
    async def save_info(self, ac_info: dict) -> None:
        """Save AC system information to database.
        
        Args:
            ac_info: Dictionary with 'acs' and 'groups' keys containing AC and group data
        """
        datetime_now = datetime.now()
        
        for ac in ac_info["acs"]:
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
        
        for group in ac_info["groups"]:
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
        ac_id: AcNumber, 
        n_last_mins: int, 
        resample_mins: int
    ) -> dict:
        """Get resampled AC points for the given AC number.
        
        Args:
            ac_id: The AC unit number to query
            n_last_mins: Look back period in minutes
            resample_mins: Resampling interval in minutes
            
        Returns:
            Dictionary with resampled AC data
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
        
        df = pd.DataFrame(ac_rows, columns=[
            'datetime', 'AcTargetSetpoint', 'Temperature'
        ])
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        
        resampled = df.resample(f'{resample_mins}min').agg({
            'AcTargetSetpoint': 'mean',
            'Temperature': 'mean'
        }).dropna(how='all')
        
        return {
            'datetime': resampled.index.to_pydatetime().tolist(),
            'AcTargetSetpoint': resampled['AcTargetSetpoint'].tolist(),
            'Temperature': resampled['Temperature'].tolist()
        }

    async def get_resampled_groups_last(
        self, 
        ac_id: AcNumber, 
        n_last_mins: int, 
        resample_mins: int,
        group_ids: List[GroupNumber] = []
    ) -> dict:
        """Get resampled group points for an AC unit.
        
        Args:
            ac_id: The AC unit number to query
            n_last_mins: Look back period in minutes
            resample_mins: Resampling interval in minutes
            group_ids: Optional list of specific group numbers to include
            
        Returns:
            Dictionary with resampled group data
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
            return {
                'datetime': [],
                'groups': []
            }
        
        df = pd.DataFrame(group_rows, columns=[
            'datetime', 'GroupNumber', 'OpenPercent', 'TargetSetpoint', 'Temperature'
        ])
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        result = {
            'datetime': [],
            'groups': []
        }
        
        for group in df['GroupNumber'].unique():
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