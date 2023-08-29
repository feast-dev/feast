from __future__ import absolute_import

from enum import Enum
from typing import Union
from pymysql.connections import Connection as _PyMySQLConnection
from sqlalchemy.engine import Connection as _DBSessionConnection

MYSQL_WRITE_RETRIES = 3
MYSQL_READ_RETRIES = 3
MYSQL_PARTITION_EXISTS_ERROR = 1517
MYSQL_DEADLOCK_ERR = 1213
MYSQL_LOCK_WAIT_TIMEOUT = 1205
MYSQL_TOO_MANY_CONNECTIONS = 1040


class ConnectionType(Enum):
    RAW = 0
    SESSION = 1


PyMySQLConnection = _PyMySQLConnection
DBSessionConnection = _DBSessionConnection
Connection = Union[PyMySQLConnection, DBSessionConnection]

FEATURE_VIEW_VERSION_TABLE_NAME = "FeatureViewVersion"
