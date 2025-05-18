"""Data destination package for async data pipeline."""

from asyncdatapipeline.destinations.api import ApiDestination
from asyncdatapipeline.destinations.base import Destination
from asyncdatapipeline.destinations.factory import (file_destination,
                                                    no_sql_destination,
                                                    sql_destination)
from asyncdatapipeline.destinations.file import FileDestination
from asyncdatapipeline.destinations.no_sql_db import MongoDBDestination
from asyncdatapipeline.destinations.sql_db import PostgreSQLDestination

__all__ = [
    "file_destination",
    "sql_destination",
    "no_sql_destination",
]
