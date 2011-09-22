class VumiError(Exception):
    pass


class InvalidMessage(VumiError):
    pass


class InvalidMessageType(VumiError):
    pass


class MissingMessageField(InvalidMessage):
    pass


class InvalidMessageField(InvalidMessage):
    pass


class ConfigError(VumiError):
    pass
