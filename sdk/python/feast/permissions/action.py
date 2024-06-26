import enum


class AuthzedAction(enum.Enum):
    """
    Identify the type of action being secured by the permissions framework, according to the familiar CRUD and Feast terminology.
    """

    ALL = "all"  # All actions
    CREATE = "create"  # Create an instance
    READ = "read"  # Access the instance state
    UPDATE = "update"  # Update the instance state
    DELETE = "delete"  # Deelete an instance
    QUERY = "query"  # Query both online and offline stores
    QUERY_ONLINE = "query_online"  # Query the online store only
    QUERY_OFFLINE = "query_offline"  # Query the offline store only
    WRITE = "write"  # Query on any store
    WRITE_ONLINE = "write_online"  # Write to the online store only
    WRITE_OFFLINE = "write_offline"  # Write to the offline store only
