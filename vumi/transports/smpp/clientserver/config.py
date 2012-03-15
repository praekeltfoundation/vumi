# -*- test-case-name: vumi.transports.smpp.clientserver.test.test_client -*-


class ClientConfig(dict):

    required_keys = [
            'host',
            'port',
            'system_id',  # in SMPP system_id is the username
            'password',
            ]

    smpp_defaults = {
            'system_type': "",
            'interface_version': "34",
            'dest_addr_ton': 0,
            'dest_addr_npi': 0,
            'registered_delivery': 0,
            }

    # SMPP v3.4 Issue 1.2 pg. 167 is wrong on id length for delivery reports
    delivery_report_regex = 'id:(?P<id>\S{,65})' \
                        + ' +sub:(?P<sub>...)' \
                        + ' +dlvrd:(?P<dlvrd>...)' \
                        + ' +submit date:(?P<submit_date>\d*)' \
                        + ' +done date:(?P<done_date>\d*)' \
                        + ' +stat:(?P<stat>[A-Z]{7})' \
                        + ' +err:(?P<err>...)' \
                        + ' +[Tt]ext:(?P<text>.{,20})' \
                        + '.*'

    client_defaults = {
            'smpp_bind_timeout': 30,
            'delivery_report_regex': delivery_report_regex,
            }

    def __init__(self, **kwargs):
        self.update(self.smpp_defaults)
        self.update(self.client_defaults)
        for key in self.required_keys:
            if key not in kwargs:
                raise ValueError("'%s' is required" % key)
        # we only want to add expected keys
        for key, value in kwargs.items():
            if key in self.required_keys \
            or key in self.smpp_defaults.keys() \
            or key in self.client_defaults.keys():
                self[key] = value
