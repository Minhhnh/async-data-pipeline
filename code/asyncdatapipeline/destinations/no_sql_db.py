from asyncdatapipeline.destinations.base import Destination
from asyncdatapipeline.monitoring import PipelineMonitor
from pymongo.asynchronous.mongo_client import AsyncMongoClient


class NoSQLDB(Destination):
    def __init__(self, db_config: dict, monitor: PipelineMonitor):
        super().__init__(monitor)

    async def connect(self):
        pass

    async def close(self):
        pass

    async def write(self, data):
        pass


class MongoDBDestination(NoSQLDB):
    """MongoDB destination for writing data to a MongoDB database."""

    def __init__(self, db_config: dict, monitor: PipelineMonitor):
        super().__init__(db_config, monitor)
        self._db_config = db_config
        self.monitor = monitor
        self._db: AsyncMongoClient
        self._credentials = db_config.get("credentials", {})

    async def connect(self):
        """Connect to the MongoDB database."""
        try:
            self._db = AsyncMongoClient(
                host=self._credentials.get("host"),
                port=self._credentials.get("port"),
                username=self._credentials.get("user"),
                password=self._credentials.get("password"),
            )
            self.monitor.log_debug("Connected to MongoDB database")
        except Exception as e:
            self.monitor.log_error(f"Connect error to MongoDB DB : {e}")
            raise

    async def send(self, data: dict) -> None:
        """Write data to MongoDB database asynchronously."""
        try:
            if not self._db:
                await self.connect()
            db = self._db.get_database(self._credentials["database"])
            collection = db.get_collection(self._db_config["collection_name"])
            await collection.insert_one(data)
            self.monitor.log_debug(f"Wrote data to {self._db_config['collection_name']} collection")
        except Exception as e:
            self.monitor.log_error(f"Error writing to MongoDB: {e}")
            raise
