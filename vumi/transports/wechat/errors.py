class WeChatException(Exception):
    pass


class UnsupportedWechatMessage(WeChatException):
    pass


class WeChatApiException(WeChatException):
    pass
