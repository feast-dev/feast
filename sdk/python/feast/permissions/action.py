import enum


class AuthzedAction(enum.Enum):
    """
    Identify the type of action being secured by the permissions framework, according to the familiar CRUD and Feast terminology.
    """

    CREATE = "create"  # Create an instance
    DESCRIBE = "describe"  # Access the instance state
    UPDATE = "update"  # Update the instance state
    DELETE = "delete"  # Deelete an instance
    QUERY_ONLINE = "query_online"  # Query the online store only
    QUERY_OFFLINE = "query_offline"  # Query the offline store only
    WRITE_ONLINE = "write_online"  # Write to the online store only
    WRITE_OFFLINE = "write_offline"  # Write to the offline store only


#  Alias for all available actions
ALL_ACTIONS = [a for a in AuthzedAction.__members__.values()]

#  Alias for all query actions
QUERY = [
    AuthzedAction.QUERY_OFFLINE,
    AuthzedAction.QUERY_ONLINE,
]
#  Alias for all write actions
WRITE = [
    AuthzedAction.WRITE_OFFLINE,
    AuthzedAction.WRITE_ONLINE,
]


#  Alias for CRUD actions
CRUD = [
    AuthzedAction.CREATE,
    AuthzedAction.DESCRIBE,
    AuthzedAction.UPDATE,
    AuthzedAction.DELETE,
]
