from typing import Any, Dict, List, Optional

import asyncpg
import dateutil.parser
from asyncdatapipeline.destinations.base import Destination
from asyncdatapipeline.monitoring import PipelineMonitor


class SQLDB(Destination):
    """RDB destination class for writing data to a relational database."""

    def __init__(self, db_config: Dict[str, Any], monitor: PipelineMonitor):
        super().__init__(monitor)
        self._db_config = db_config
        self._connection: Optional[asyncpg.Connection] = None


class PostgreSQLDestination(SQLDB):
    """PostgreSQL destination for writing data to a PostgreSQL database."""

    def __init__(self, db_config: Dict[str, Any], monitor: PipelineMonitor, table_name: str = "", columns: List[str] = None):
        super().__init__(db_config, monitor)
        self._connection: asyncpg.Connection
        self.table_name = table_name
        self.columns = columns if columns is not None else []

    async def connect(self) -> asyncpg.Connection:
        """Connect to the PostgreSQL database."""
        try:
            self._connection = await asyncpg.connect(**self._db_config)
            self.monitor.log_debug("Connected to PostgreSQL database")
        except Exception as e:
            self.monitor.log_error(f"Connect error to Postgres DB : {e}")
            raise
        return self._connection

    async def reconnect(self) -> asyncpg.Connection:
        """Reconnect to the PostgreSQL database."""
        if self._connection:
            await self._connection.close()
        return await self.connect()

    async def send(self, data: Dict[str, Any]) -> None:
        """Write data to PostgreSQL database asynchronously."""
        try:
            if not self._connection:
                await self.connect()
            if self.table_name and self.columns:
                data['timestamp'] = dateutil.parser.parse(data['timestamp']).replace(tzinfo=None)
                data['created_at'] = dateutil.parser.parse(data['created_at']).replace(tzinfo=None)
                data['retweets'] = int(data['retweets'])
                data['likes'] = int(data['likes'])
                columns = ", ".join(self.columns)
                values = ", ".join([f"${i + 1}" for i in range(len(self.columns))])
                query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({values})"
                await self._connection.execute(query, *[data[col] for col in self.columns])
            else:
                raise ValueError("Table name or columns not specified")
            self.monitor.log_debug(f"Wrote data to {self.table_name} table")
        except ValueError as ve:
            self.monitor.log_error(f"Value error: {ve}")
            raise
        except Exception as e:
            self.monitor.log_error(f"Error writing to PostgreSQL database: {e}")
            raise
