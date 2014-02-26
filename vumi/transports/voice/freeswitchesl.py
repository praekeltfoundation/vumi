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
from twisted.internet import defer, reactor

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
        self._ctx = None
        self._rawlen = None
        self._io = StringIO()
        self._crlf = re.compile(r"[\r\n]+")
        self._raw_response = [
            "api/response",
            "text/disconnect-notice",
        ]

    def send(self, cmd):
        #log.msg("send %s" % cmd)
        if isinstance(cmd, types.UnicodeType):
            cmd = cmd.encode("utf-8")
        self.transport.write(cmd + "\n\n")

    def raw_send(self, stuff):
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

    def process_line(self, ev, line):
        try:
            k, v = self._crlf.sub("", line).split(":", 1)
            k = k.replace("-", "_").strip()
            v = urllib.unquote(v.strip())
            ev[k] = v
        except:
            pass

    def parse_event(self, isctx=False):
        ev = _O()
        self._io.reset()

        for line in self._io:
            if line == "\n":
                break
            self.process_line(ev, line)

        if not isctx:
            rawlength = ev.get("Content_Length")
            if rawlength:
                ev.rawresponse = self._io.read(int(rawlength))

        self._io.reset()
        self._io.truncate()
        return ev

    def read_raw_response(self):
        self._io.reset()
        chunk = self._io.read(int(self._ctx.Content_Length))
        self._io.reset()
        self._io.truncate()
        return _O(rawresponse=chunk)

    def dispatchEvent(self, ctx, event):
        ctx.data = _O(event.copy())
        reactor.callLater(0, self.event_received, _O(ctx.copy()))
        self._ctx = self._rawlen = None

    def event_received(self, ctx):
        pass

    def lineReceived(self, line):
        if line:
            self._io.write(line + "\n")
        else:
            ctx = self.parse_event(True)
            rawlength = ctx.get("Content_Length")
            if rawlength:
                self._ctx = ctx
                self._rawlen = int(rawlength)
                self.setRawMode()
            else:
                self.dispatchEvent(ctx, _O())

    def rawDataReceived(self, data):
        if self._rawlen is None:
            rest = ""
        else:
            data, rest = data[:self._rawlen], data[self._rawlen:]
            self._rawlen -= len(data)

        self._io.write(data)
        if self._rawlen == 0:
            if self._ctx.get("Content_Type") in self._raw_response:
                self.dispatchEvent(self._ctx, self.read_raw_response())
            else:
                self.dispatchEvent(self._ctx, self.parse_event())
            self.setLineMode(rest)


class FreeSwitchEventProtocol(FreeSwitchEventSocket):

    def __init__(self):
        FreeSwitchEventSocket.__init__(self)

        # our internal event queue
        self._event_queue = []

        # callbacks by event's content-type
        self.__EventCallbacks = {
            "auth/request": self.auth_request,
            "api/response": self._api_response,
            "command/reply": self._command_reply,
            "text/event-plain": self._plain_event,
            "text/disconnect-notice": self.on_disconnect,
        }

    def _protocol_send(self, name, args=""):
        deferred = defer.Deferred()
        self._event_queue.append((name, deferred))
        self.send("%s %s" % (name, args))
        return deferred

    def _protocol_send_raw(self, name, args=""):
        deferred = defer.Deferred()
        self._event_queue.append((name, deferred))
        self.raw_send("%s %s" % (name, args))
        return deferred

    def _protocol_sendmsg(self, name, args=None, uuid="", lock=False):
        deferred = defer.Deferred()
        self._event_queue.append((name, deferred))
        self.sendmsg(name, args, uuid, lock)
        return deferred

    def event_received(self, ctx):
        content_type = ctx.get("Content_Type", None)
        log.msg("GOT EVENT: %s" % (ctx,))
        if content_type:
            method = self.__EventCallbacks.get(content_type, None)
            if callable(method):
                return method(ctx)
            else:
                return self.unknown_content_type(content_type, ctx)

    def auth_request(self, ctx):
        log.msg("auth_request received")

    def on_disconnect(self, ctx):
        log.msg("on_disconnect received")

    def _api_response(self, ctx):
        cmd, deferred = self._event_queue.pop(0)
        if cmd == "api":
            deferred.callback(ctx)
        else:
            deferred.errback(
                EventError(
                    "apiResponse on '%s': out of sync?" %
                    cmd))

    def _command_reply(self, ctx):
        if not self._event_queue:  # an unexpected reply ignore it
            return
        cmd, deferred = self._event_queue.pop(0)
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

    def _plain_event(self, ctx):
        name = ctx.data.get("Event_Name")
        if name:
            evname = "on" + string.capwords(name, "_").replace("_", "")

        method = getattr(self, evname, None)
        if callable(method):
            return method(ctx.data)
        else:
            return self.unboundEvent(ctx.data, evname)

    def unknown_content_type(self, content_type, ctx):
        log.err("[freeswitchesl] unknown Content-Type: %s" % content_type,
                logLevel=log.logging.DEBUG)

    def unboundEvent(self, ctx, evname):
        log.err("[freeswitchesl] unbound Event: %s" % evname,
                logLevel=log.logging.DEBUG)

    # EVENT SOCKET COMMANDS
    def api(self, args):
        "Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#api"
        return self._protocol_send("api", args)

    def sendevent(self, name, args=dict(), body=None):
        "Please refer to http://wiki.freeswitch.org/wiki/"
        "Event_Socket#sendevent"
        parsed_args = [name]
        for k, v in args.iteritems():
            parsed_args.append('%s: %s' % (k, v))
        parsed_args.append('')
        if body:
            parsed_args.append(body)
        return self._protocol_send_raw("sendevent", '\n'.join(parsed_args))

    def bgapi(self, args):
        "Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#bgapi"
        return self._protocol_send("bgapi", args)

    def exit(self):
        "Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#exit"
        return self._protocol_send("exit")

    def eventplain(self, args):
        "Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#event"
        return self._protocol_send('eventplain', args)

    def event(self, args):
        "Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#event"
        return self._protocol_sendmsg("event", args, lock=True)

    def linger(self, args=None):
        "Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#event"
        return self._protocol_send("linger", args)

    def filter(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#filter

        The user might pass any number of values to filter an event for.
        But, from the point filter() is used, just the filtered events
        will come to the app - this is where this
        function differs from event().

         filter('Event-Name MYEVENT')
         filter('Unique-ID 4f37c5eb-1937-45c6-b808-6fba2ffadb63')
        """
        return self._protocol_send('filter', args)

    def filter_delete(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/Event_Socket
        #filter_delete

        filter_delete('Event-Name MYEVENT')
        """
        return self._protocol_send('filter delete', args)

    def verbose_events(self):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_verbose_events

        verbose_events()
        """
        return self._protocol_sendmsg('verbose_events', lock=True)

    def auth(self, args):
        """Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#auth

        This method is allowed only for Inbound connections."""
        return self._protocol_send("auth", args)

    def connect(self):
        "Please refer to http://wiki.freeswitch.org/wiki/"
        "Event_Socket_Outbound#Using_Netcat"
        return self._protocol_send("connect")

    def myevents(self):
        "Please refer to http://wiki.freeswitch.org/wiki/"
        "Event_Socket#event"
        return self._protocol_send("myevents")

    def answer(self):
        "Please refer to http://wiki.freeswitch.org/wiki/"
        "Event_Socket_Outbound#Using_Netcat"
        return self._protocol_sendmsg("answer", lock=True)

    def bridge(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/Event_Socket_Outbound
        bridge("{ignore_early_media=true}sofia/gateway/myGW/177808")
        """
        return self._protocol_sendmsg("bridge", args, lock=True)

    def hangup(self, reason=""):
        """Hangup may be used by both Inbound and Outbound connections.

        When used by Inbound connections, you may add the extra `reason`
        argument.
        Please refer to http://wiki.freeswitch.org/wiki/Event_Socket#hangup
        for details.

        """
        return self._protocol_sendmsg("hangup", reason, lock=True)

    def sched_api(self, args):
        "Please refer to http://wiki.freeswitch.org/wiki/"
        "Mod_commands#sched_api"
        return self._protocol_sendmsg("sched_api", args, lock=True)

    def ring_ready(self):
        "Please refer to http://wiki.freeswitch.org/wiki/"
        "Misc._Dialplan_Tools_ring_ready"
        return self._protocol_sendmsg("ring_ready")

    def record_session(self, filename):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_record_session
        """
        return self._protocol_sendmsg("record_session", filename, lock=True)

    def read(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_read
        """
        return self._protocol_sendmsg("read", args, lock=True)

    def bind_meta_app(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_bind_meta_app
        """
        return self._protocol_sendmsg("bind_meta_app", args, lock=True)

    def wait_for_silence(self, args):
        """Please refer to http://wiki.freeswitch.org/wiki/
           Misc._Dialplan_Tools_wait_for_silence
        """
        return self._protocol_sendmsg("wait_for_silence", args, lock=True)

    def sleep(self, milliseconds):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_sleep
        """
        return self._protocol_sendmsg("sleep", milliseconds, lock=True)

    def vmd(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/Mod_vmd
        """
        return self._protocol_sendmsg("vmd", args, lock=True)

    def set(self, args):
        """Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_set
        set("ringback=${us-ring}")
        """
        return self._protocol_sendmsg("set", args, lock=True)

    def set_global(self, args):
        """Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_set_global
        """
        return self._protocol_sendmsg("set_global", args, lock=True)

    def unset(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_unset
        """
        return self._protocol_sendmsg("unset", args, lock=True)

    def start_dtmf(self):
        """Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_start_dtmf
        """
        return self._protocol_sendmsg("start_dtmf", lock=True)

    def stop_dtmf(self):
        """Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_stop_dtmf
        """
        return self._protocol_sendmsg("stop_dtmf", lock=True)

    def start_dtmf_generate(self):
        """Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_start_dtmf_generate
        """
        return self._protocol_sendmsg("start_dtmf_generate", "true", lock=True)

    def stop_dtmf_generate(self):
        """Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_stop_dtmf_generate
        """
        return self._protocol_sendmsg("stop_dtmf_generate", lock=True)

    def queue_dtmf(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_queue_dtmf
        """
        return self._protocol_sendmsg("queue_dtmf", args, lock=True)

    def flush_dtmf(self):
        """Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_flush_dtmf
        """
        return self._protocol_sendmsg("flush_dtmf", lock=True)

    def play_fsv(self, filename):
        """Please refer to http://wiki.freeswitch.org/wiki/Mod_fsv

        play_fsv("/tmp/video.fsv")
        """
        return self._protocol_sendmsg("play_fsv", filename, lock=True)

    def record_fsv(self, filename):
        """Please refer to http://wiki.freeswitch.org/wiki/Mod_fsv

        record_fsv("/tmp/video.fsv")
        """
        return self._protocol_sendmsg("record_fsv", filename, lock=True)

    def playback(self, filename, terminators=None, lock=True):
        """Please refer to http://wiki.freeswitch.org/wiki/Mod_playback

        The optional argument `terminators` may contain a string with
        the characters that will terminate the playback.

        playback("/tmp/dump.gsm", terminators="#8")

        In this case, the audio playback is automatically terminated
        by pressing either '#' or '8'.
        """
        self.set("playback_terminators=%s" % terminators or "none")
        return self._protocol_sendmsg("playback", filename, lock=lock)

    def transfer(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_transfer

        transfer("3222 XML default")
        """
        return self._protocol_sendmsg("transfer", args, lock=True)

    def conference(self, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Mod_conference#API_Reference

        conference("myconf")
        """
        return self._protocol_sendmsg("conference", args, lock=True)

    def att_xfer(self, url):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_att_xfer

        att_xfer("user/1001")
        """
        return self._protocol_sendmsg("att_xfer", url, lock=True)

    def send_break(self):
        return self._protocol_sendmsg("break", lock=True)

    def endless_playback(self, filename):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Misc._Dialplan_Tools_endless_playback

        endless_playback("/tmp/dump.gsm")
        """
        return self._protocol_sendmsg("endless_playback", filename, lock=True)

    def execute(self, command, args):
        """
        Please refer to http://wiki.freeswitch.org/wiki/
        Event_Socket_Library#execute

        execute('voicemail', 'default $${domain} 1000')
        """
        return self._protocol_sendmsg(command, args, lock=True)
