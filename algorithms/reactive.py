import logging
from typing import List

from utils import config

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
            T_groups_current: List[float],
            T_groups_history: List[List[float]],
            interval_history: int,
            airflow_groups_current: List[float],
            overshoot_max: float,
            lag_max: float
    ):
        n_rooms = len(T_groups_current)
        n_history = len(T_groups_history)

        # ΔT_current = T_current - T_target
        temp_diffs_current = [T - T_target for T in T_groups_current]
        avg_temp_diff_current = sum(temp_diffs_current) / n_rooms if n_rooms else 0

        # Compute temperature change rate per room using full history
        temp_change_rates = []
        for index_group in range(n_rooms):
            rates = []
            for t in range(1, n_history):
                delta = T_groups_history[t][index_group] - T_groups_history[t - 1][index_group]
                rates.append(delta / interval_history)
            avg_rate = sum(rates) / len(rates) if rates else 0.0
            temp_change_rates.append(avg_rate)

        # Aggregate metrics
        avg_temp_change_rate = sum(temp_change_rates) / n_rooms if n_rooms else 0.0
        heating_mode = (mode_ac == "heat")
        cooling_mode = (mode_ac == "cool")

        # Stable baseline
        T_ac_target_next = T_ac_target_current

        if heating_mode:
            if avg_temp_diff_current < -lag_max and avg_temp_change_rate < 0.05:
                T_ac_target_next = min(T_ac_target_current + 1, T_max)
            elif avg_temp_diff_current > overshoot_max or avg_temp_change_rate > 0.2:
                T_ac_target_next = max(T_ac_target_current - 1, T_target)
        elif cooling_mode:
            if avg_temp_diff_current > lag_max and avg_temp_change_rate > -0.05:
                T_ac_target_next = max(T_ac_target_current - 1, T_min)
            elif avg_temp_diff_current < -overshoot_max or avg_temp_change_rate < -0.2:
                T_ac_target_next = min(T_ac_target_current + 1, T_target)

        airflow_groups_next = []
        for index_group, (temp_diff_current, rate, airflow_current) in enumerate(
                zip(temp_diffs_current, temp_change_rates, airflow_groups_current)):
            airflow = airflow_current
            if heating_mode:
                if temp_diff_current < -lag_max and rate < 0.05:
                    airflow = 1.0
                elif temp_diff_current > 0 or rate > 0.2:
                    airflow = max(0.1, airflow_current * 0.7)
            elif cooling_mode:
                if temp_diff_current > lag_max and rate > -0.05:
                    airflow = 1.0
                elif temp_diff_current < 0 or rate < -0.2:
                    airflow = max(0.1, airflow_current * 0.7)
            else:
                airflow = 0.0
            airflow_groups_next.append(airflow)

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
        T_groups_current: List[float], 
        T_groups_history: List[List[float]], 
        interval_history: int, 
        airflow_groups_current: List[float]
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
                self.config.get("overshoot_max", 1.5), 
                self.config.get("lag_max", 0.2)
            )
