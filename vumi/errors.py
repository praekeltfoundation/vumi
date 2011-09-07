class VumiError(Exception):
    pass


class InvalidMessage(VumiError):
    pass


class MissingMessageField(InvalidMessage):
    pass


class InvalidMessageField(InvalidMessage):
    pass
