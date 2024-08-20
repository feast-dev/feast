import enum


class AuthzedAction(enum.Enum):
    """
    Identify the type of action being secured by the permissions framework, according to the familiar CRUD and Feast terminology.
    """

    CREATE = "create"  # Create an instance
    DESCRIBE = "describe"  # Access the instance state
    UPDATE = "update"  # Update the instance state
    DELETE = "delete"  # Delete an instance
    READ_ONLINE = "read_online"  # Read the online store only
    READ_OFFLINE = "read_offline"  # Read the offline store only
    WRITE_ONLINE = "write_online"  # Write to the online store only
    WRITE_OFFLINE = "write_offline"  # Write to the offline store only


#  Alias for all available actions
ALL_ACTIONS = [a for a in AuthzedAction.__members__.values()]

#  Alias for all read actions
READ = [
    AuthzedAction.READ_OFFLINE,
    AuthzedAction.READ_ONLINE,
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
