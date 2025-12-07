import asyncio
from datetime import datetime, date, time, timedelta
import logging

import numpy as np

from database import AsyncMySQLConnector
from utils import config

class Daemon:
    def __init__(self) -> None:
        self.logger = logging.getLogger('logger')
        self.config = config.get_config().get("daemon")
        
        self.ac_type = self.config.get("ac_type")
        self.algorithm_type = self.config.get("algorithm_type")
        self.apis_enabled = self.config.get("apis_enabled", [])
        
        self.algorithm = None
        self.apis = {}

        self.setup_complete = False
        self.setup_sleep = 5
        
        self.db_base = AsyncMySQLConnector(
            host=self.config.get("db_host"), 
            user=self.config.get("db_user"), 
            password=self.config.get("db_password"), 
            db=self.config.get("db_name")
        )
        
        self.running = False
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
    async def sleep_until_next_n_minutes(self, n_minutes: int) -> None:
        if 60 % n_minutes != 0:
            raise ValueError("n_minutes must divide 60 (e.g. 1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, or 60)")

        datetime_now = datetime.now()
        minutes_past = datetime_now.minute % n_minutes
        minutes_to_next = n_minutes - minutes_past if minutes_past != 0 else n_minutes
        datetime_next = (datetime_now + timedelta(minutes=minutes_to_next)).replace(second=0, microsecond=0)
        delay = (datetime_next - datetime_now).total_seconds()
        await asyncio.sleep(delay)
    
    async def init_ac(self) -> None:
        if self.ac_type == "AirTouch":
            import drivers.airtouch
        
            self.ac = drivers.airtouch.AirTouchAC(iot_ip=self.config.get("iot_ip"), db_base=self.db_base)

            self.on_ai = {ac_id: False for ac_id in await self.ac.get_ac_ids()}

            await self.ac.ensure_tables()
        else:
            raise ValueError(f"Unknown AC type '{self.ac_type}'")
        
    async def init_algorithm(self) -> None:
        if self.algorithm_type == "reactive":
            import algorithms.reactive
            
            algorithm_version = self.config.get("algorithm_version")
            self.algorithm = algorithms.reactive.Reactive(version=algorithm_version)
        else: 
            raise ValueError(f"Unknown algorithm type '{self.algorithm_type}'")
        
    async def init_apis(self) -> None:
        if "solar" in self.apis_enabled:
            import api.solar
            self.apis["solar"] = api.solar.Solar()

    async def setup_loop(self) -> None:
        await self.db_base.connect()
        await self.init_ac()
        await self.init_algorithm()
        await self.init_apis()
        
        self.setup_complete = True
        
    async def loop_update_config(self) -> None:
        while self.running:
            try:
                self.config = config.get_config().get("daemon")
                self.logger.info("Config successfully updated")
            except Exception as e:
                self.logger.error(f"Failed to update config: {str(e)}")
            await self.sleep_until_next_n_minutes(5)

    async def loop_save_ac_info(self) -> None:
        while self.running:
            try:
                if not self.setup_complete:
                    self.logger.info(f"Setup not complete, sleeping for {self.setup_sleep} seconds")
                    await asyncio.sleep(self.setup_sleep)
                    continue
                
                await self.ac.save_info()
            except Exception as e:
                self.logger.error(f"Failed to save AC info: {str(e)}")
            
            await self.sleep_until_next_n_minutes(5)
            
    async def loop_set_ac_power(self) -> None:
        while self.running:
            try:
                if not self.setup_complete:
                    self.logger.info(f"Setup not complete, sleeping for {self.setup_sleep} seconds")
                    await asyncio.sleep(self.setup_sleep)
                    continue
                
                def is_period_end_boundary(end_time: datetime.time, now: datetime.time, grace_minutes: int = 5) -> bool:
                    today = datetime.today()
                    end_dt = datetime.combine(today, end_time)
                    now_dt = datetime.combine(today, now)

                    # Handle rollover past midnight
                    if end_dt < datetime.combine(today, datetime.min.time()):
                        end_dt += timedelta(days=1)

                    return end_dt <= now_dt < end_dt + timedelta(minutes=grace_minutes)
                
                async def log_set_ac_power(ac_id: int, power_on: bool) -> None:
                    self.on_ai[ac_id] = True if power_on else False
                    await self.ac.set_ac_power(ac_id, power_on)

                # Parse configured time periods
                period_daytime = [datetime.strptime(t, "%H:%M").time() for t in self.config.get("periods", {}).get("daytime", [])]
                if len(period_daytime) != 2:
                    self.logger.error(f"Invalid period_daytime config: {period_daytime}")
                    await self.sleep_until_next_n_minutes(5)
                    continue
                mode_daytime = self.config.get("periods_modes", {}).get("daytime", "Cool")
                eT_trigger_daytime = self.config.get("periods_eT_trigger", {}).get("daytime")

                period_morning = [datetime.strptime(t, "%H:%M").time() for t in self.config.get("periods", {}).get("morning", [])]
                if len(period_morning) != 2:
                    self.logger.error(f"Invalid period_morning config: {period_morning}")
                    await self.sleep_until_next_n_minutes(5)
                    continue
                mode_morning = self.config.get("periods_modes", {}).get("morning", "Heat")
                eT_trigger_morning = self.config.get("periods_eT_trigger", {}).get("morning")

                time_now = datetime.now().time()
                is_period_daytime = period_daytime[0] <= time_now < period_daytime[1]
                is_period_morning = period_morning[0] <= time_now < period_morning[1] 

                T_target = self.config.get("T_target")
                power_consumption = self.config.get("power_consumption", 0)

                consumption_net = self.apis.get("solar").get_consumption_net()
                ac_ids = await self.ac.get_ac_ids()

                for ac_id in ac_ids:
                    try:                       
                        mode_ac = await self.ac.get_mode_ac(ac_id=ac_id)
                        T_groups = await self.ac.get_T_groups(ac_id=ac_id)
                        mean_T_groups = np.mean([float(value) for value in T_groups.values()])
                        max_T_group = max([float(value) for value in T_groups.values()])
                        min_T_group = min([float(value) for value in T_groups.values()])

                        if is_period_daytime:
                            self.logger.debug("Inside daytime period")
                            if mode_ac == mode_daytime:
                                if consumption_net < -(0.60 * power_consumption) and max_T_group > T_target + eT_trigger_daytime:
                                    self.logger.info(f"Turning on AC {ac_id} ({mode_ac}) due to excess solar ({consumption_net}) and high temperature ({max_T_group})")
                                    await log_set_ac_power(ac_id=ac_id, power_on=True)
                                elif consumption_net > (0.60 * power_consumption):
                                    if self.on_ai.get(ac_id):
                                        self.logger.info(f"Turning off AC {ac_id} ({mode_ac}) due to high power consumption ({consumption_net})")
                                        await log_set_ac_power(ac_id=ac_id, power_on=False)
                                else:
                                    self.logger.debug(f"Maintaining current power state for AC {ac_id} ({mode_ac})")
                        elif is_period_morning:
                            self.logger.debug("Inside morning period")
                            if mode_ac == mode_morning:
                                if min_T_group < T_target - eT_trigger_morning:
                                    self.logger.info(f"Turning on AC {ac_id} ({mode_ac}) due to low temperature ({min_T_group})")
                                    await log_set_ac_power(ac_id=ac_id, power_on=True)
                                elif min_T_group > T_target:
                                    if self.on_ai.get(ac_id):
                                        self.logger.info(f"Turning off AC {ac_id} ({mode_ac}) due to high temperature ({min_T_group})")
                                        await log_set_ac_power(ac_id=ac_id, power_on=False)
                                else:
                                    self.logger.debug(f"Maintaining current power state for AC {ac_id} ({mode_ac})")
                        else:
                            # Outside any controlled period → allow manual control
                            # BUT turn off AC immediately when the period just ended
                            if is_period_end_boundary(period_daytime[1], time_now) or is_period_end_boundary(period_morning[1], time_now):
                                if self.on_ai.get(ac_id):
                                    self.logger.info(f"Period just ended → Turning off AC {ac_id}")
                                    await log_set_ac_power(ac_id=ac_id, power_on=False)
                            else:
                                self.logger.debug(f"Outside control periods → leaving AC {ac_id} in manual mode")

                        self.logger.debug(f"Successfully handled AC {ac_id}")

                    except Exception as e:
                        self.logger.error(f"Failed to set power state for AC {ac_id}: {str(e)}")
                        continue

            except Exception as e:
                self.logger.error(f"Failed in AC power loop: {str(e)}")
            
            await self.sleep_until_next_n_minutes(1)

                
    async def loop_run_algorithm(self) -> None:
        while self.running:
            try:
                if not self.setup_complete:
                    self.logger.info(f"Setup not complete, sleeping for {self.setup_sleep} seconds")
                    await asyncio.sleep(self.setup_sleep)
                    continue
                
                # Get current time and determine if we're in sleep mode (10PM-6AM)
                time_now = datetime.now().time()
                is_sleep = time(hour=0, minute=0) <= time_now < time(hour=6, minute=0) #TODO: Make times load from config
                
                # Apply temperature override during sleep hours
                T_target_base = self.config.get("T_target")
                T_target_sleep = self.config.get("T_target_sleep")
                T_target = T_target_sleep if is_sleep else T_target_base

                ac_ids = await self.ac.get_ac_ids()
                for ac_id in ac_ids:
                    try:
                        params_algorithm_reactive = await self.ac.get_params_algorithm_reactive(ac_id=ac_id)
                        mode_ac = params_algorithm_reactive.get("mode_ac")
                        T_min = params_algorithm_reactive.get("T_min")
                        T_max = params_algorithm_reactive.get("T_max")
                        T_ac_target_current = params_algorithm_reactive.get("T_ac_target_current")
                        T_ac_in_current = params_algorithm_reactive.get("T_ac_in_current")
                        T_ac_in_history = params_algorithm_reactive.get("T_ac_in_history")
                        group_ids = params_algorithm_reactive.get("group_ids", [])
                        T_groups_current = params_algorithm_reactive.get("T_groups_current")
                        T_groups_history = params_algorithm_reactive.get("T_groups_history")
                        interval_history = params_algorithm_reactive.get("interval_history")
                        airflow_groups_current = params_algorithm_reactive.get("airflow_groups_current")

                        T_ac_target_next, airflow_groups_next = self.algorithm.step(
                            mode_ac=mode_ac,
                            T_target=T_target,
                            T_min=T_min,
                            T_max=T_max,
                            T_ac_target_current=T_ac_target_current,
                            T_ac_in_current=T_ac_in_current,
                            T_ac_in_history=T_ac_in_history,
                            T_groups_current=T_groups_current,
                            T_groups_history=T_groups_history,
                            interval_history=interval_history,
                            airflow_groups_current=airflow_groups_current
                        )

                        airflow_groups_next = {group_id: airflow for group_id, airflow in zip(group_ids, airflow_groups_next)}

                        await self.ac.set_T_ac_target(ac_id=ac_id, T_ac_target=T_ac_target_next)
                        await self.ac.set_airflow_groups(ac_id=ac_id, airflow_groups=airflow_groups_next)

                        self.logger.info(f"Successfully sent command to AC {ac_id}")
                    
                    except Exception as e:
                        self.logger.error(f"Failed to process AC {ac_id}: {str(e)}")
                        continue
                
            except Exception as e:
                self.logger.error(f"Failed in algorithm loop: {str(e)}")
                
            await self.sleep_until_next_n_minutes(1)

    async def loop_root(self) -> None:
        self.running = True
        await asyncio.gather(
            self.loop_update_config(), 
            self.setup_loop(), 
            self.loop_save_ac_info(), 
            self.loop_set_ac_power(), 
            self.loop_run_algorithm()
        )

    def start(self) -> None:
        def stop_loop(*_):
            print("Stopping event loop")
            self.running = False
            # Cancels all tasks to allow graceful shutdown
            for task in asyncio.all_tasks(loop=self.loop):
                task.cancel()

        try:
            self.loop.run_until_complete(self.loop_root())
        except asyncio.CancelledError:
            pass
        finally:
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            self.loop.close()
            print("Loop closed")
