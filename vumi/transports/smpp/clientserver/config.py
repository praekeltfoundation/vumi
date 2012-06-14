# -*- test-case-name: vumi.transports.smpp.clientserver.tests.test_config -*-

import re
import inspect


class ClientConfig(object):

    # SMPP v3.4 Issue 1.2 pg. 167 is wrong on id length for delivery reports
    DELIVERY_REPORT_REGEX = 'id:(?P<id>\S{,65})' \
                        + ' +sub:(?P<sub>...)' \
                        + ' +dlvrd:(?P<dlvrd>...)' \
                        + ' +submit date:(?P<submit_date>\d*)' \
                        + ' +done date:(?P<done_date>\d*)' \
                        + ' +stat:(?P<stat>[A-Z]{7})' \
                        + ' +err:(?P<err>...)' \
                        + ' +[Tt]ext:(?P<text>.{,20})' \
                        + '.*'

    def __init__(self, host, port, system_id, password,
                 system_type="", interface_version="34", service_type="",
                 dest_addr_ton=0, dest_addr_npi=0,
                 source_addr_ton=0, source_addr_npi=0,
                 registered_delivery=0, smpp_bind_timeout=30,
                 smpp_enquire_link_interval=55.0,
                 initial_reconnect_delay=5.0,
                 delivery_report_regex=None):
        # in SMPP system_id is the username
        self.host = host
        self.port = port
        self.system_id = system_id
        self.password = password
        self.system_type = system_type
        self.interface_version = interface_version
        self.service_type = service_type
        self.dest_addr_ton = int(dest_addr_ton)
        self.dest_addr_npi = int(dest_addr_npi)
        self.source_addr_ton = int(source_addr_ton)
        self.source_addr_npi = int(source_addr_npi)
        self.registered_delivery = int(registered_delivery)
        self.smpp_bind_timeout = int(smpp_bind_timeout)
        self.smpp_enquire_link_interval = float(smpp_enquire_link_interval)
        self.initial_reconnect_delay = float(initial_reconnect_delay)
        if delivery_report_regex is None:
            delivery_report_regex = self.DELIVERY_REPORT_REGEX
        self.delivery_report_re = re.compile(delivery_report_regex)

    def __eq__(self, other):
        if not isinstance(other, ClientConfig):
            return False
        return self.__dict__ == other.__dict__

    @classmethod
    def from_config(cls, config):
        """Convert a config dictionary to a sanitized ClientConfig
           object by stripping out any extra keys.
           """
        argspec = inspect.getargspec(cls.__init__)
        kwargs = dict((k, config[k]) for k in argspec.args
                      if k in config and k != 'self')
        return cls(**kwargs)

    def to_dict(self):
        """Return a dictionary representing the config."""
        return self.__dict__.copy()
