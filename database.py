import aiomysql

class AsyncMySQLConnector:
    def __init__(self, host, user, password, db, port=3306):
        self.config = {
            "host": host,
            "user": user,
            "password": password,
            "db": db,
            "port": port,
        }
        self.pool = None

    async def connect(self):
        self.pool = await aiomysql.create_pool(**self.config)

    async def close(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    async def execute_query(self, query, params=None):
        """Use for SELECT queries. Returns all fetched rows."""
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(query, params or ())
                result = await cur.fetchall()
                return result

    async def execute_command(self, command, params=None):
        """Use for INSERT/UPDATE/DELETE. Returns number of affected rows."""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(command, params or ())
                await conn.commit()
                return cur.rowcount

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()
