"""Data destination package for async data pipeline."""

from asyncdatapipeline.destinations.factory import (file_destination,
                                                    no_sql_destination,
                                                    sql_destination)

__all__ = [
    "file_destination",
    "sql_destination",
    "no_sql_destination",
]
