import asyncio
from datetime import datetime, timedelta
import logging

from database import AsyncMySQLConnector
from utils import config

class Daemon:
    def __init__(self, ac_type: str = "AirTouch") -> None:
        self.logger = logging.getLogger('logger')
        self.config = config.get_config().get("daemon")
        
        self.ac_type = ac_type
        
        self.setup_complete = False
        self.setup_sleep = 10
        
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
        
            self.ac = drivers.airtouch.AirTouchAC(iot_ip=self.config.get("iot_ip"))
            self.db = drivers.airtouch.AirTouchDB(db=self.db_base)
            
            await self.db.ensure_tables()
        else:
            raise ValueError(f"Unknown AC type '{self.ac_type}'")
        
    async def setup_loop(self) -> None:
        await self.db_base.connect()
        await self.init_ac()
        
        self.setup_complete = True

    async def loop_pull_ac_info(self) -> None:
        while self.running:
            if not self.setup_complete:
                self.logger.info(f"Setup not complete, sleeping for {self.setup_sleep} seconds")
                await asyncio.sleep(self.setup_sleep)
                continue
            
            self.ac_info = await self.ac.get_info()
            
            await self.db.save_info(ac_info=self.ac_info)
            self.logger.info("AC info successfully saved to DB")
            
            await self.sleep_until_next_n_minutes(5)

    async def loop_root(self) -> None:
        self.running = True
        await asyncio.gather(
            self.setup_loop(), 
            self.loop_pull_ac_info(),
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
