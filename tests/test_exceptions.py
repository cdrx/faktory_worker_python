import pytest

from faktory.exceptions import (
    FaktoryAuthenticationError,
    FaktoryConnectionResetError,
    FaktoryError,
    FaktoryHandshakeError,
)


@pytest.mark.parametrize(
    "exc",
    [FaktoryAuthenticationError, FaktoryConnectionResetError, FaktoryHandshakeError],
)
def test_errors_are_caught_with_connection_errors(exc: Exception):
    """
    Tests to make sure that handling a `ConnectionError` will allow
    us to also catch the custom, more description exceptions we
    have implemented.
    """

    with pytest.raises(ConnectionError):
        raise exc


@pytest.mark.parametrize(
    "exc",
    [FaktoryAuthenticationError, FaktoryConnectionResetError, FaktoryHandshakeError],
)
def test_inherits_from_base_project_exception(exc: Exception):
    """
    Tests to make sure that all custom exceptions that we have
    defined are inheriting from our base `FaktoryError` class,
    so that when someone handles a `FaktoryError`, any of the
    subclasses will be caught appropriately.
    """

    with pytest.raises(FaktoryError):
        raise exc
