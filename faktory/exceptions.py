__ALL__ = ["FaktoryHandshakeError", "FaktoryAuthenticationError", "FaktoryError"]


class FaktoryError(Exception):
    """
    The base Faktory Exception that all other Faktory related
    exceptions inherit from. To catch any other, more specific,
    Faktory exception, catch this one and handle appropriately.

    This Exception should not be used, and is in place strictly
    as a way to catch all `Faktory` exceptions.
    """

    pass


class FaktoryHandshakeError(FaktoryError, ConnectionError):
    pass


class FaktoryAuthenticationError(FaktoryError, ConnectionError):
    pass


class FaktoryConnectionResetError(FaktoryError, ConnectionResetError):
    pass
