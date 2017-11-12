__ALL__ = ['FaktoryHandshakeError', 'FaktoryAuthenticationError']


class FaktoryHandshakeError(ConnectionError):
    pass


class FaktoryAuthenticationError(ConnectionError):
    pass
