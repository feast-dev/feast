
class OnlineStoreError(Exception):
    pass

class OnlineStoreMinorError(Exception):
    """
    A minor error is an error that independently impacts a component of Galactus:
        * One RPC request (write, read),
        *
    and which throwing an exception would impact non-effected components of the system.

    A minor error is an error that impacts a subset of FeatureViews independently and is conditioned on
    the state of the store in a specific environment (e.g. prod or stage).

    Minor errors should not block deploys.

    Action: triggers rollbars
    """
    pass


class OnlineStoreMajorError(Exception):
    """
    A major error is an error that has service-wide impact.

    Majors errors should block deploys to maximize the change they're caught in staging.
    """
    pass
