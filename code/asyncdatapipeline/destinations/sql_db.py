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


class PostgreSQLDestination(SQLDB):
    """PostgreSQL destination for writing data to a PostgreSQL database."""

    def __init__(self, db_config: Dict[str, Any], monitor: PipelineMonitor, table_name: str = "", columns: List[str] = None):
        super().__init__(db_config, monitor)
        self.table_name = table_name
        self.columns = columns if columns is not None else []

    async def connect(self) -> asyncpg.Connection:
        """Connect to the PostgreSQL database."""
        try:
            connection = await asyncpg.connect(**self._db_config)
            self.monitor.log_debug("Connected to PostgreSQL database")
        except Exception as e:
            self.monitor.log_error(f"Connect error to Postgres DB : {e}")
            raise
        return connection

    async def send(self, data: Dict[str, Any]) -> None:
        """Write data to PostgreSQL database asynchronously."""
        connection: asyncpg.Connection = None
        try:
            if not connection:
                connection = await self.connect()
            if self.table_name and self.columns:
                if isinstance(data['timestamp'], str):
                    data['timestamp'] = dateutil.parser.parse(data['timestamp']).replace(tzinfo=None)
                if isinstance(data['created_at'], str):
                    data['created_at'] = dateutil.parser.parse(data['created_at']).replace(tzinfo=None)
                data['retweets'] = int(data['retweets'])
                data['likes'] = int(data['likes'])
                columns = ", ".join(self.columns)
                values = ", ".join([f"${i + 1}" for i in range(len(self.columns))])
                query = f"INSERT INTO {self.table_name} ({columns}) VALUES ({values})"
                await connection.execute(query, *[data[col] for col in self.columns])
            else:
                raise ValueError("Table name or columns not specified")
            self.monitor.log_debug(f"Wrote data to {self.table_name} table")
        except ValueError as ve:
            self.monitor.log_error(f"Value error: {ve}")
            raise
        except Exception as e:
            self.monitor.log_error(f"Error writing to PostgreSQL database: {e}")
            raise
