# coding: utf-8
#
# Twisted protocol for the FreeSWITCH's Event Socket
#

"Twisted protocol for the FreeSWITCH Event Socket"

import types
import string
import re
import urllib
from cStringIO import StringIO
from twisted.python import log
from twisted.protocols import basic
from twisted.internet import defer, reactor, protocol

__version__ = "0.1"


class EventError(Exception):
    pass


class AuthError(Exception):
    pass


class _O(dict):

    """Translates dictionary keys to instance attributes"""

    def __setattr__(self, k, v):
        dict.__setitem__(self, k, v)

    def __delattr__(self, k):
        dict.__delitem__(self, k)

    def __getattribute__(self, k):
        try:
            return dict.__getitem__(self, k)
        except KeyError:
            return dict.__getattribute__(self, k)


class FreeSwitchEventSocket(basic.LineReceiver):
    delimiter = "\n"

    def __init__(self):
        self.__ctx = None
        self.__rawlen = None
        self.__io = StringIO()
        self.__crlf = re.compile(r"[\r\n]+")
        self.__rawresponse = [
            "api/response",
            "text/disconnect-notice",
        ]

    def send(self, cmd):
        #log.msg("send %s" % cmd)
        if isinstance(cmd, types.UnicodeType):
            cmd = cmd.encode("utf-8")
        self.transport.write(cmd + "\n\n")

    def rawSend(self, stuff):
        if isinstance(stuff, types.UnicodeType):
            stuff = stuff.encode('utf-8')
        self.transport.write(stuff)

    def sendmsg(self, name, arg=None, uuid="", lock=False):
        if isinstance(name, types.UnicodeType):
            name = name.encode("utf-8")
        if isinstance(arg, types.UnicodeType):
            arg = arg.encode("utf-8")

        self.transport.write("sendmsg %s\ncall-command: execute\n" % uuid)
        self.transport.write("execute-app-name: %s\n" % name)
        if arg:
            self.transport.write("execute-app-arg: %s\n" % arg)
        if lock is True:
            self.transport.write("event-lock: true\n")

        self.transport.write("\n\n")

    def processLine(self, ev, line):
        try:
            k, v = self.__crlf.sub("", line).split(":", 1)
            k = k.replace("-", "_").strip()
            v = urllib.unquote(v.strip())
            ev[k] = v
        except:
            pass

    def parseEvent(self, isctx=False):
        ev = _O()
        self.__io.reset()

        for line in self.__io:
            if line == "\n":
                break
            self.processLine(ev, line)

        if not isctx:
            rawlength = ev.get("Content_Length")
            if rawlength:
                ev.rawresponse = self.__io.read(int(rawlength))

        self.__io.reset()
        self.__io.truncate()
        return ev

    def readRawResponse(self):
        self.__io.reset()
        chunk = self.__io.read(int(self.__ctx.Content_Length))
        self.__io.reset()
        self.__io.truncate()
        return _O(rawresponse=chunk)

    def dispatchEvent(self, ctx, event):
        ctx.data = _O(event.copy())
        reactor.callLater(0, self.eventReceived, _O(ctx.copy()))
        self.__ctx = self.__rawlen = None

    def eventReceived(self, ctx):
        msg.log("event recevied" + repr(ctx))

    def lineReceived(self, line):
        if line:
            self.__io.write(line + "\n")
        else:
            ctx = self.parseEvent(True)
            rawlength = ctx.get("Content_Length")
            if rawlength:
                self.__ctx = ctx
                self.__rawlen = int(rawlength)
                self.setRawMode()
            else:
                self.dispatchEvent(ctx, _O())

    def rawDataReceived(self, data):
        if self.__rawlen is None:
            rest = ""
        else:
            data, rest = data[:self.__rawlen], data[self.__rawlen:]
            self.__rawlen -= len(data)

        self.__io.write(data)
        if self.__rawlen == 0:
            if self.__ctx.get("Content_Type") in self.__rawresponse:
                self.dispatchEvent(self.__ctx, self.readRawResponse())
            else:
                self.dispatchEvent(self.__ctx, self.parseEvent())
            self.setLineMode(rest)


class FreeSwitchEventProtocol(FreeSwitchEventSocket):

    def __init__(self):
        FreeSwitchEventSocket.__init__(self)

        # our internal event queue
        self.__EventQueue = []

        # callbacks by event's content-type
        self.__EventCallbacks = {
            "auth/request": self.authRequest,
            "api/response": self._apiResponse,
            "command/reply": self._commandReply,
            "text/event-plain": self._plainEvent,
            "text/disconnect-notice": self.onDisconnect,
        }

    def __protocolSend(self, name, args=""):
        deferred = defer.Deferred()
        self.__EventQueue.append((name, deferred))
        self.send("%s %s" % (name, args))
        return deferred

    def __protocolSendRaw(self, name, args=""):
        deferred = defer.Deferred()
        self.__EventQueue.append((name, deferred))
        self.rawSend("%s %s" % (name, args))
        return deferred

    def __protocolSendmsg(self, name, args=None, uuid="", lock=False):
        deferred = defer.Deferred()
        self.__EventQueue.append((name, deferred))
        self.sendmsg(name, args, uuid, lock)
        return deferred

    def eventReceived(self, ctx):
        content_type = ctx.get("Content_Type", None)
        log.msg("GOT EVENT: %s" % (ctx,))
        if content_type:
            method = self.__EventCallbacks.get(content_type, None)
            if callable(method):
                return method(ctx)
            else:
                return self.unknownContentType(content_type, ctx)

    def authRequest(self, ctx):
        log.msg("Authrequest received")

    def onDisconnect(self, ctx):
        log.msg("OnDisconnect received")

    def _apiResponse(self, ctx):
        cmd, deferred = self.__EventQueue.pop(0)
        if cmd == "api":
            deferred.callback(ctx)
        else:
            deferred.errback(
                EventError(
                    "apiResponse on '%s': out of sync?" %
                    cmd))

    def _commandReply(self, ctx):
        if not self.__EventQueue:  # an unexpected reply ignore it
            return
        cmd, deferred = self.__EventQueue.pop(0)
        # on connection define our UUID
        if cmd == "connect":
            log.msg("Connect Reply " + ctx.variable_call_uuid)
            self.uniquecallid = ctx.variable_call_uuid

        if ctx.Reply_Text.startswith("+OK"):
            deferred.callback(ctx)
        elif cmd == "auth":
            deferred.errback(AuthError("invalid password"))
        else:
            deferred.errback(EventError(ctx))

    def _plainEvent(self, ctx):
        name = ctx.data.get("Event_Name")
        if name:
            evname = "on" + string.capwords(name, "_").replace("_", "")

        method = getattr(self, evname, None)
        if callable(method):
            return method(ctx.data)
        else:
            return self.unboundEvent(ctx.data, evname)

    def unknownContentType(self, content_type, ctx):
        log.err("[freeswitchesl] unknown Content-Type: %s" % content_type,
                logLevel=log.logging.DEBUG)

    def unboundEvent(self, ctx, evname):
        log.err("[freeswitchesl] unbound Event: %s" % evname,
                logLevel=log.logging.DEBUG)

    # EVENT SOCKET COMMANDS
    def api(self, args):
        "Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#api"
        return self.__protocolSend("api", args)

    def sendevent(self, name, args=dict(), body=None):
        "Please refer to http://wiki.freeswitch.org/wiki/"
        "Event_Socket#sendevent"
        parsed_args = [name]
        for k, v in args.iteritems():
            parsed_args.append('%s: %s' % (k, v))
        parsed_args.append('')
        if body:
            parsed_args.append(body)
        return self.__protocolSendRaw("sendevent", '\n'.join(parsed_args))

    def bgapi(self, args):
        "Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#bgapi"
        return self.__protocolSend("bgapi", args)

    def exit(self):
        "Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#exit"
        return self.__protocolSend("exit")

    def eventplain(self, args):
        "Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#event"
        return self.__protocolSend('eventplain', args)

    def event(self, args):
        "Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#event"
        return self.__protocolSendmsg("event", args, lock=True)

    def linger(self, args=None):
        "Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#event"
        return self.__protocolSend("linger", args)

    def filter(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#filter

        The user might pass any number of values to filter an event for.
        But, from the point filter() is used, just the filtered events
        will come to the app - this is where this
        function differs from event().

        >>> filter('Event-Name MYEVENT')
        >>> filter('Unique-ID 4f37c5eb-1937-45c6-b808-6fba2ffadb63')
        """
        return self.__protocolSend('filter', args)

    def filter_delete(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/Event_Socket
        #filter_delete

        >>> filter_delete('Event-Name MYEVENT')
        """
        return self.__protocolSend('filter delete', args)

    def verbose_events(self):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_verbose_events

        >>> verbose_events()
        """
        return self.__protocolSendmsg('verbose_events', lock=True)

    def auth(self, args):
        """Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#auth

        This method is allowed only for Inbound connections."""
        return self.__protocolSend("auth", args)

    def connect(self):
        "Please refer to http://wiki.freeswitch.org/wiki/"
        "Event_Socket_Outbound#Using_Netcat"
        return self.__protocolSend("connect")

    def myevents(self):
        "Please refer to http://wiki.freeswitch.org/wiki/"
        "Event_Socket#event"
        return self.__protocolSend("myevents")

    def answer(self):
        "Please refer to http://wiki.freeswitch.org/wiki/"
        "Event_Socket_Outbound#Using_Netcat"
        return self.__protocolSendmsg("answer", lock=True)

    def bridge(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/Event_Socket_Outbound
        >>> bridge("{ignore_early_media=true}sofia/gateway/myGW/177808")
        """
        return self.__protocolSendmsg("bridge", args, lock=True)

    def hangup(self, reason=""):
        """Hangup may be used by both Inbound and Outbound connections.

        When used by Inbound connections, you may add the extra `reason`
        argument.
        Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#hangup
        for details.

        """
        return self.__protocolSendmsg("hangup", reason, lock=True)

    def sched_api(self, args):
        "Please refer to http://wiki.freeswitch.org/wiki/"
        "Mod_commands#sched_api"
        return self.__protocolSendmsg("sched_api", args, lock=True)

    def ring_ready(self):
        "Please refer to http://wiki.freeswitch.org/wiki/"
        "Misc._Dialplan_Tools_ring_ready"
        return self.__protocolSendmsg("ring_ready")

    def record_session(self, filename):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_record_session
        """
        return self.__protocolSendmsg("record_session", filename, lock=True)

    def read(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_read
        """
        return self.__protocolSendmsg("read", args, lock=True)

    def bind_meta_app(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_bind_meta_app
        """
        return self.__protocolSendmsg("bind_meta_app", args, lock=True)

    def wait_for_silence(self, args):
        """Please refer to http://wiki.freeswitch.org/wiki/
           Misc._Dialplan_Tools_wait_for_silence
        """
        return self.__protocolSendmsg("wait_for_silence", args, lock=True)

    def sleep(self, milliseconds):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_sleep
        """
        return self.__protocolSendmsg("sleep", milliseconds, lock=True)

    def vmd(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/Mod_vmd
        """
        return self.__protocolSendmsg("vmd", args, lock=True)

    def set(self, args):
        """Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_set
        >>> set("ringback=${us-ring}")
        """
        return self.__protocolSendmsg("set", args, lock=True)

    def set_global(self, args):
        """Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_set_global
        """
        return self.__protocolSendmsg("set_global", args, lock=True)

    def unset(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_unset
        """
        return self.__protocolSendmsg("unset", args, lock=True)

    def start_dtmf(self):
        """Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_start_dtmf
        """
        return self.__protocolSendmsg("start_dtmf", lock=True)

    def stop_dtmf(self):
        """Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_stop_dtmf
        """
        return self.__protocolSendmsg("stop_dtmf", lock=True)

    def start_dtmf_generate(self):
        """Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_start_dtmf_generate
        """
        return self.__protocolSendmsg("start_dtmf_generate", "true", lock=True)

    def stop_dtmf_generate(self):
        """Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_stop_dtmf_generate
        """
        return self.__protocolSendmsg("stop_dtmf_generate", lock=True)

    def queue_dtmf(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_queue_dtmf
        """
        return self.__protocolSendmsg("queue_dtmf", args, lock=True)

    def flush_dtmf(self):
        """Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_flush_dtmf
        """
        return self.__protocolSendmsg("flush_dtmf", lock=True)

    def play_fsv(self, filename):
        """Please refer to http://wiki.freeswitch.org/wiki/Mod_fsv

        >>> play_fsv("/tmp/video.fsv")
        """
        return self.__protocolSendmsg("play_fsv", filename, lock=True)

    def record_fsv(self, filename):
        """Please refer to http://wiki.freeswitch.org/wiki/Mod_fsv

        >>> record_fsv("/tmp/video.fsv")
        """
        return self.__protocolSendmsg("record_fsv", filename, lock=True)

    def playback(self, filename, terminators=None, lock=True):
        """Please refer to http://wiki.freeswitch.org/wiki/Mod_playback

        The optional argument `terminators` may contain a string with
        the characters that will terminate the playback.

        >>> playback("/tmp/dump.gsm", terminators="#8")

        In this case, the audio playback is automatically terminated
        by pressing either '#' or '8'.
        """
        self.set("playback_terminators=%s" % terminators or "none")
        return self.__protocolSendmsg("playback", filename, lock=lock)

    def transfer(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_transfer

        >>> transfer("3222 XML default")
        """
        return self.__protocolSendmsg("transfer", args, lock=True)

    def conference(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Mod_conference#API_Reference

        >>> conference("myconf")
        """
        return self.__protocolSendmsg("conference", args, lock=True)

    def att_xfer(self, url):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_att_xfer

        >>> att_xfer("user/1001")
        """
        return self.__protocolSendmsg("att_xfer", url, lock=True)

    def send_break(self):
        return self.__protocolSendmsg("break", lock=True)

    def endless_playback(self, filename):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_endless_playback

        >>> endless_playback("/tmp/dump.gsm")
        """
        return self.__protocolSendmsg("endless_playback", filename, lock=True)

    def execute(self, command, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Event_Socket_Library#execute

        >>> execute('voicemail', 'default $${domain} 1000')
        """
        return self.__protocolSendmsg(command, args, lock=True)


__all__ = ['EventError', 'AuthError', 'EventSocket', 'EventProtocol']
