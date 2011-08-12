from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.internet import task
import pygsm
import serial

from vumi.service import Worker
from vumi.message import Message


class GsmTransport(Worker):

    @inlineCallbacks
    def startWorker(self):

        log.msg("Starting the GsmTransport, config: %s" % self.config)
        # create the publisher
        self.publisher = yield self.publish_to(
            'gsm.inbound.%(msisdn)s' % self.config)
        # subscribe routing keys to callbacks
        self.consume('gsm.outbound.%(msisdn)s' % self.config,
                     self.handle_outbound_message)

        self.modem = pygsm.GsmModem(port=self.config.get('port'),
                                    logger=self.logger)

        self.looper = task.LoopingCall(self.check_for_inbound_message)
        self.looper.start(self.config.get('interval'))

    def modem_is_connected(self):
        return (hasattr(self.modem, "device") and
                (self.modem.device is not None))

    @inlineCallbacks
    def check_for_inbound_message(self):
        try:
            log.msg('checking for inbound message')
            if self.modem_is_connected():
                msg = self.modem.next_message()
                if msg is not None:
                    yield self.handle_inbound_message(msg)
        except serial.SerialException:
            yield self.modem.reboot()

    def modem_logger(self, obj, msg, prefix):
        self.logger('%s - %s: %s' % (obj.__class__.__name__, prefix, msg))

    def logger(self, *a):
        if self.config.get('debug'):
            log.msg(*a)

    def handle_inbound_message(self, sms):
        log.msg("Received '%s' from '%s' at '%s'" % (sms.text, sms.sender,
                                                     sms.received))
        self.publisher.publish_message(Message(sender=sms.sender,
            recipient=self.config.get('msisdn'),
            content=sms.text,
            received=sms.received))

    def handle_outbound_message(self, msg):
        if not self.modem_is_connected():
            return False
        self.modem.send_sms(**msg.payload)

    def debug_inbound(self, message):
        log.msg('Received an SMS: %s' % message.payload)

    def stopWorker(self):
        if self.modem_is_connected():
            self.modem.disconnect()
        log.msg("Stopping the GsmWorker")
