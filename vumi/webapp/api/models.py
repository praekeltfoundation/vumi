from django.db import models
from django.contrib.auth.models import User
from django.contrib import admin
from utils import model_to_tuples, model_to_dict
import fields

CLICKATELL_ERROR_CODES = (
    (001, 'Authentication failed'),
    (002, 'Unknown username or password'),
    (003, 'Session ID expired'),
    (004, 'Account frozen'),
    (005, 'Missing session ID'),
    (007, 'IP Lockdown violation'),  # You have locked down the API instance to
                                     # a specific IP address and then sent from
                                     # an IP address different to the one you
                                     # set.
    (101, 'Invalid or missing parameters'),
    (102, 'Invalid user data header'),
    (103, 'Unknown API message ID'),
    (104, 'Unknown client message ID'),
    (105, 'Invalid destination address'),
    (106, 'Invalid source address'),
    (107, 'Empty message'),
    (108, 'Invalid or missing API ID'),
    (109, 'Missing message ID'),  # This can be either a client message ID or
                                  # API message ID. For example when using the
                                  # stop message command.
    (110, 'Error with email message'),
    (111, 'Invalid protocol'),
    (112, 'Invalid message type'),
    (113, 'Maximum message parts'),  # The text message component of the
                                     # message is greater than exceeded the
                                     # permitted 160 characters (70 Unicode
                                     # characters). Select concat equal to
                                     # 1,2,3-N to overcome this by splitting
                                     # the message across multiple messages.
    (114, 'Cannot route message'),  # This implies that the gateway is not
                                    # currently routing messages to this
                                    # network prefix. Please email
                                    # support@clickatell.com with the mobile
                                    # number in question.
    (115, 'Message expired'),
    (116, 'Invalid Unicode data'),
    (120, 'Invalid delivery time'),
    (121, 'Destination mobile number'),  # This number is not allowed to
                                         # receive messages from us and blocked
                                         # has been put on our block list.
    (122, 'Destination mobile opted out'),
    (123, 'Invalid Sender ID'),  # A sender ID needs to be registered and
                                 # approved before it can be successfully used
                                 # in message sending.
    (128, 'Number delisted'),  # This error may be returned when a number has
                               # been delisted.
    (201, 'Invalid batch ID'),
    (202, 'No batch template'),
    (301, 'No credit left'),
    (302, 'Max allowed credit'),
)

CLICKATELL_MESSAGE_STATUSES = (
    (0, 'Pending locally'),  # this is our own status
    (1, 'Message unknown'),  # everything above zero is clickatell's status
                             # codes
    (2, 'Message queued'),
    (3, 'Delivered to gateway'),
    (4, 'Received by recipient'),
    (5, 'Error with message'),
    (6, 'User cancelled message delivery'),
    (7, 'Error delivering message'),
    (8, 'OK'),
    (9, 'Routing error'),
    (10, 'Message expired'),
    (11, 'Message queued for later delivery'),
    (12, 'Out of credit'),
)


class SentSMSBatch(models.Model):
    """A set of Messages to be sent through Vumi"""
    user = models.ForeignKey(User)
    title = models.CharField(blank=False, max_length=100)
    created_at = models.DateTimeField(blank=True, auto_now_add=True)
    updated_at = models.DateTimeField(blank=True, auto_now=True)

    class Meta:
        ordering = ['-created_at']
        get_latest_by = 'created_at'
        verbose_name = 'Sent SMS Batch'
        verbose_name_plural = 'Sent SMS Batches'

    def __unicode__(self):
        return u"SentSMSBatch %s: %s (%s) @ %s" % (self.id,
                                            self.title,
                                            self.user,
                                            self.created_at)


class SentSMS(models.Model):
    """An Message to be sent through Vumi"""
    user = models.ForeignKey(User)
    batch = models.ForeignKey(SentSMSBatch, blank=True, null=True)
    to_msisdn = models.CharField(blank=False, max_length=100)
    from_msisdn = models.CharField(blank=False, max_length=100)
    charset = models.CharField(blank=True, default='utf8', max_length=32)
    message = models.CharField(blank=False, max_length=160)
    transport_name = models.CharField(blank=False, max_length=256)
    transport_msg_id = models.CharField(blank=True, default='', max_length=256)
    transport_status = models.CharField(blank=True, default='', max_length=256)
    created_at = models.DateTimeField(blank=True, auto_now_add=True)
    updated_at = models.DateTimeField(blank=True, auto_now=True)
    delivered_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        ordering = ['-created_at']
        get_latest_by = 'created_at'
        verbose_name = 'Sent SMS'

    def __unicode__(self):
        return u"SentSMS %s -> %s, %s:%s @ %s" % (self.from_msisdn,
                                            self.to_msisdn,
                                            self.transport_name,
                                            self.transport_status,
                                            self.delivered_at)


class SMPPLink(models.Model):
    sent_sms = models.ForeignKey(SentSMS, unique=True)
    sequence_number = models.IntegerField(db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-created_at']
        get_latest_by = 'created_at'
        verbose_name = 'SMPP Link'

    def __unicode__(self):
        return u"SMPPLink %s -> %s @ %s" % (self.sent_sms,
                                            self.sequence_number,
                                            self.created_at)


class SMPPResp(models.Model):
    sent_sms = models.ForeignKey(SentSMS, unique=True)
    sequence_number = models.IntegerField()
    command_id = models.CharField(blank=False, max_length=100)
    command_status = models.CharField(blank=False, max_length=100)
    message_id = models.CharField(blank=False, max_length=100)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-created_at']
        get_latest_by = 'created_at'
        verbose_name = 'SMPP Response'

    def __unicode__(self):
        return u"SMPPLink %s : %s = %s @ %s" % (self.sent_sms,
                                            self.command_id,
                                            self.command_status,
                                            self.created_at)


class ReceivedSMS(models.Model):
    user = models.ForeignKey(User)
    to_msisdn = models.CharField(max_length=32)
    from_msisdn = models.CharField(max_length=32)
    charset = models.CharField(blank=True, default='utf8', max_length=32)
    message = models.CharField(max_length=160)
    transport_name = models.CharField(blank=False, max_length=255)
    transport_msg_id = models.CharField(blank=True, default='', max_length=255)
    received_at = models.DateTimeField(blank=False)
    created_at = models.DateTimeField(blank=True, auto_now_add=True)
    updated_at = models.DateTimeField(blank=True, auto_now=True)

    class Meta:
        ordering = ['-created_at']
        get_latest_by = 'created_at'
        verbose_name = 'Received SMS'

    def as_dict(self):
        """Return variables ready made for a URL callback"""
        return model_to_dict(self)

    def as_tuples(self):
        return model_to_tuples(self)

    def __unicode__(self):
        return u"ReceivedSMS %s -> %s @ %s" % (self.from_msisdn,
                                                self.to_msisdn,
                                                self.received_at)


class Profile(models.Model):
    """An API user's profile"""
    user = models.ForeignKey(User, unique=True)
    created_at = models.DateTimeField(blank=True, auto_now_add=True)
    updated_at = models.DateTimeField(blank=True, auto_now=True)
    transport = models.ForeignKey('Transport', null=True, blank=False)

    def __unicode__(self):
        return u"Profile for %s" % self.user


CALLBACK_CHOICES = (
    ('sms_received', 'SMS Received'),
    ('sms_receipt', 'SMS Receipt'),
)


class URLCallback(models.Model):
    """A URL to with to post data for an event"""
    profile = models.ForeignKey(Profile)
    name = models.CharField(blank=True, max_length=255,
                            choices=CALLBACK_CHOICES)
    url = fields.AuthenticatedURLField(blank=True, verify_exists=False)
    created_at = models.DateTimeField(blank=True, auto_now_add=True)
    updated_at = models.DateTimeField(blank=True, auto_now=True)

    class Meta:
        verbose_name = 'Callback URL'
        ordering = ['created_at']

    def __unicode__(self):
        return u"URLCallback %s - %s" % (self.name, self.url)


class Transport(models.Model):
    name = models.CharField(blank=True, max_length=255)

    def __unicode__(self):
        return u"Transport: %s" % self.name


class Keyword(models.Model):
    """An SMS keyword"""
    keyword = models.CharField(blank=True, max_length=255)
    user = models.ForeignKey('auth.User')

    def __unicode__(self):
        return u"Keyword: %s for %s" % (self.keyword, self.user)

    def save(self, *args, **kwargs):
        self.keyword = self.keyword.lower()
        super(Keyword, self).save(*args, **kwargs)


admin.site.register(SentSMS)
admin.site.register(SentSMSBatch)
admin.site.register(ReceivedSMS)
admin.site.register(Profile)
admin.site.register(URLCallback)
admin.site.register(SMPPLink)
admin.site.register(SMPPResp)
admin.site.register(Transport)
admin.site.register(Keyword)

# import signals to make sure they're registered as soon
# as we start working with the models.
import signals
