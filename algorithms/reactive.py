import logging
import math
from typing import List

from utils import config
from utils.util import sigmoid

class Reactive:
    def __init__(self, version="v1_0"):
        self.version = version
        self.logger = logging.getLogger('logger')
        self.config = config.get_config().get("algorithms", {}).get("reactive", {})
        
    def v1_0(
            self,
            mode_ac: str, 
            T_target: int, 
            T_min: int, 
            T_max: int, 
            T_ac_target_current: int, 
            T_ac_in_current: float, 
            T_ac_in_history: int, 
            T_groups_current: dict, 
            T_groups_history: List[List[float]], 
            interval_history: int, 
            airflow_groups_current: List[float], 
            airflow_group_min: float, 
            airflow_group_max: float, 
            airflow_ramp_degree: float,
            group_ids: List[int]
    ):
        # Convert T_groups_current dict to list in group_ids order, stripping to float
        T_groups_current_list = [float(T_groups_current[group_id]) for group_id in group_ids]
        
        n_groups = len(T_groups_current_list)
        if n_groups < 1:
            self.logger.warning("No valid groups for current AC → Skipping command generation")
            return T_ac_target_current, [airflow_group_min] * len(group_ids)
        
        n_history = len(T_groups_history)

        # eT_current = T_current - T_target
        eT_groups_current = [T_group_current - T_target for T_group_current in T_groups_current_list]
        eT_groups_mean_current = sum(eT_groups_current) / n_groups if n_groups else 0

        # Compute temperature change rate per room using full history
        dT_groups_rate = []
        for index_group in range(n_groups):
            rates = []
            for t in range(1, n_history):
                try:
                    eT_group = T_groups_history[t][index_group] - T_groups_history[t - 1][index_group]
                except Exception as e:
                    eT_group = 0.0
                    self.logger.error(f"Error computing dT for group {index_group} at time {t}: {e}")
                rates.append(eT_group / interval_history)
            dT_group_rate = sum(rates) / len(rates) if rates else 0.0
            dT_groups_rate.append(dT_group_rate)

        heating_mode = (mode_ac == "heat")
        cooling_mode = (mode_ac == "cool")

        # Initialize target setpoint
        T_ac_target_next = T_ac_target_current
        airflow_groups_mean_current = sum(airflow_groups_current) / n_groups
        airflow_groups_max_current = max(airflow_groups_current) if airflow_groups_current else 0.0
        airflow_groups_metric_current = (airflow_groups_mean_current + airflow_groups_max_current) / 2
        airflow_groups_next = []

        # First: decide setpoint
        if heating_mode:
            T_max_soft = int(T_ac_in_current + 1) if T_ac_in_current.is_integer() else int(math.ceil(T_ac_in_current))
            if airflow_groups_metric_current >= 0.6:
                T_ac_target_next = max(T_target, max(T_ac_in_current, min(T_ac_target_current + 1, T_max_soft)))
            elif airflow_groups_metric_current <= 0.4:
                T_ac_target_next = max(T_ac_in_current - 1, T_ac_target_current - 1)
            else:
                T_ac_target_next = T_ac_target_current
        elif cooling_mode:
            T_min_soft = int(T_ac_in_current - 1) if T_ac_in_current.is_integer() else int(math.floor(T_ac_in_current))
            if airflow_groups_metric_current >= 0.6: # TODO: Remove magic numbers
                T_ac_target_next = min(min(max(T_min_soft, T_ac_target_current - 1), T_ac_in_current + 1), T_target)
            elif airflow_groups_metric_current <= 0.4:
                T_ac_target_next = min(T_ac_target_current + 1, T_ac_in_current + 1)
            else:
                T_ac_target_next = T_ac_target_current

        T_ac_target_next = max(T_min, min(T_ac_target_next, T_max))
        T_ac_target_next = int(T_ac_target_next)

        # Then: decide airflow per room
        for index_group, (T_group_current, dT_group_rate, airflow_group_current) in enumerate(
                zip(T_groups_current_list, dT_groups_rate, airflow_groups_current)):
            eT_group = T_group_current - T_target
            airflow_group_next = airflow_group_current  # default value (i.e. no need for fan if no heating/cooling required)

            if heating_mode:
                if eT_group < 0:
                    airflow_group_next = airflow_group_max
                else:
                    airflow_group_next = sigmoid(eT_group)
                    airflow_group_next = min(max(airflow_group_min, airflow_group_next), airflow_group_max)
            elif cooling_mode:
                if eT_group > 0:
                    airflow_group_next = airflow_group_max
                else:
                    airflow_group_next = sigmoid(-eT_group)
                    airflow_group_next = min(max(airflow_group_min, airflow_group_next), airflow_group_max)

            airflow_groups_next.append(round(airflow_group_next, 2))

        self.logger.info(
            f"Generated command: T_ac_target_next={T_ac_target_next}, airflow_groups_next={airflow_groups_next}")

        return T_ac_target_next, airflow_groups_next

    def step(
        self,
        mode_ac: str,
        T_target: int, 
        T_min: int, 
        T_max: int, 
        T_ac_target_current: int, 
        T_ac_in_current: float, 
        T_ac_in_history: int, 
        T_groups_current: dict, 
        T_groups_history: List[List[float]], 
        interval_history: int, 
        airflow_groups_current: List[float],
        group_ids: List[int]
    ):
        if self.version == "v1_0":
            return self.v1_0(
                mode_ac,
                T_target, 
                T_min, 
                T_max, 
                T_ac_target_current, 
                T_ac_in_current, 
                T_ac_in_history, 
                T_groups_current, 
                T_groups_history, 
                interval_history, 
                airflow_groups_current, 
                self.config.get("airflow_group_min", 0.1), 
                self.config.get("airflow_group_max", 1.0), 
                self.config.get("airflow_ramp_degree", 0.1),
                group_ids
            )
