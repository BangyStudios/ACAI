import asyncio
from datetime import datetime, timedelta
import logging

from database import AsyncMySQLConnector
from utils import config

class Daemon:
    def __init__(self) -> None:
        self.logger = logging.getLogger('logger')
        self.config = config.get_config().get("daemon")
        
        self.ac_type = self.config.get("ac_type")
        self.algorithm_type = self.config.get("algorithm_type")
        
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
        
    async def setup_loop(self) -> None:
        await self.db_base.connect()
        await self.init_ac()
        await self.init_algorithm()
        
        self.setup_complete = True
        
    async def loop_update_config(self) -> None:
        while self.running:
            self.config = config.get_config().get("daemon")
            self.logger.info("Config successfully updated")
            await self.sleep_until_next_n_minutes(5)

    async def loop_save_ac_info(self) -> None:
        while self.running:
            if not self.setup_complete:
                self.logger.info(f"Setup not complete, sleeping for {self.setup_sleep} seconds")
                await asyncio.sleep(self.setup_sleep)
                continue
            
            await self.ac.save_info()
            
            await self.sleep_until_next_n_minutes(5)
            
    # async def get_last_n
            
    async def loop_run_algorithm(self) -> None:
        while self.running:
            if not self.setup_complete:
                self.logger.info(f"Setup not complete, sleeping for {self.setup_sleep} seconds")
                await asyncio.sleep(self.setup_sleep)
                continue
            
            T_target = self.config.get("T_target")

            ac_ids_on = await self.ac.get_ac_ids_on()
            for ac_id in ac_ids_on:
                (
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
                ) = await self.ac.get_params_algorithm_reactive(ac_id=ac_id)

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

                await self.ac.set_T_ac_target(ac_id=ac_id, T_ac_target=T_ac_target_next)
                await self.ac.set_airflow_groups(ac_id=ac_id, airflow_groups=airflow_groups_next)

                self.logger.info(f"Successfully sent command to AC: {ac_id}")
            
            await self.sleep_until_next_n_minutes(5)

    async def loop_root(self) -> None:
        self.running = True
        await asyncio.gather(
            self.loop_update_config(), 
            self.setup_loop(), 
            self.loop_save_ac_info(),
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
