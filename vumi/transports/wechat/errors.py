class WeChatException(Exception):
    pass


class UnsupportedWeChatMessage(WeChatException):
    pass


class WeChatApiException(WeChatException):
    pass


class WeChatParserException(WeChatException):
    pass
