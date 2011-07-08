from textwrap import wrap

from twisted.conch.insults.insults import TerminalProtocol, privateModes, ServerProtocol
from twisted.conch.insults.window import TopWindow, VBox, TextInput, TextOutput
from twisted.internet import reactor, protocol
from twisted.conch.telnet import TelnetTransport, TelnetBootstrapProtocol, TelnetProtocol
from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.service import Worker
from vumi.message import Message



class InputWidget(TextInput):
    def __init__(self, maxwidth, onSubmit):
        self._realSubmit = onSubmit
        super(InputWidget, self).__init__(maxwidth, self._onSubmit)

    def _onSubmit(self, line):
        if line:
            self.setText('')
        self._realSubmit(line)


class OutputWidget(TextOutput):
    def __init__(self, size=None):
        super(OutputWidget, self).__init__(size)
        self.messages = []

    def formatMessage(self, s, width):
        return wrap(s, width=width, subsequent_indent="  ")

    def addMessage(self, message):
        self.messages.append(message)
        self.repaint()

    def render(self, width, height, terminal):
        output = []
        for i in xrange(len(self.messages) - 1, -1, -1):
            output[:0] = self.formatMessage(self.messages[i], width - 2)
            if len(output) >= height:
                break
        if len(output) < height:
            output[:0] = [''] * (height - len(output))
        for n, L in enumerate(output):
            terminal.cursorPosition(0, n)
            terminal.write(L + ' ' * (width - len(L)))


class UserInterface(TerminalProtocol):
    width = 80
    height = 24

    reactor = reactor

    def __init__(self, setUI, inputHandler, *args, **kw):
        setUI(self)
        self.inputHandler = inputHandler
        super(TerminalProtocol, self).__init__(*args, **kw)

    def connectionMade(self):
        super(UserInterface, self).connectionMade()
        self.terminal.eraseDisplay()
        self.terminal.resetPrivateModes([privateModes.CURSOR_MODE])
        self.rootWidget = self.mkWidgets()
        self.outputWidget = self.rootWidget.children[0].children[0]

    def mkWidgets(self):
        def _schedule(f):
            self.reactor.callLater(0, f)
        root = TopWindow(self._painter, _schedule)
        root.reactor = self.reactor
        vbox = VBox()
        vbox.addChild(OutputWidget())
        vbox.addChild(InputWidget(self.width - 2, self.parseInputLine))
        root.addChild(vbox)
        return root

    def _painter(self):
        self.rootWidget.draw(self.width, self.height, self.terminal)

    def addOutputMessage(self, msg):
        self.outputWidget.addMessage(msg)

    def parseInputLine(self, line):
        if line.lower() == '/quit':
            self.quit()
        else:
            self.addOutputMessage("-> " + line)
            if self.inputHandler:
                self.inputHandler(line)

    def keystrokeReceived(self, keyID, modifier):
        self.rootWidget.keystrokeReceived(keyID, modifier)

    def terminalSize(self, width, height):
        self.width = width
        self.height = height
        self._painter()

    def quit(self):
        self.terminal.setPrivateModes([privateModes.CURSOR_MODE])
        self.terminal.loseConnection()

    def connectionLost(self, reason):
        log.msg("Client lost connection.")



class ThinUI(TelnetProtocol):
    def __init__(self, setUI, inputHandler, *args, **kw):
        setUI(self)
        self.inputHandler = inputHandler

    def dataReceived(self, data):
        data = data.rstrip('\r\n')
        if data.lower() == '/quit':
            self.quit()
        else:
            self.addOutputMessage("-> " + data)
            if self.inputHandler:
                self.inputHandler(data)

    def quit(self):
        self.loseConnection()

    def addOutputMessage(self, msg):
        self.transport.write(msg + '\r\n')


class TelnetConsoleTransport(Worker):
    ui = None

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting console transport.")
        self.publisher = yield self.publish_to('console.inbound')
        self.consume('console.outbound', self.consume_message)

        reactor.listenTCP(1234, self.getFactory())
        log.msg("Started service")

    def getFactory(self, use_thin=True):
        f = protocol.ServerFactory()
        if use_thin:
            f.protocol = lambda: TelnetTransport(ThinUI, self.setUI, self.handleInput)
        else:
            f.protocol = lambda: TelnetTransport(TelnetBootstrapProtocol, ServerProtocol, UserInterface, self.setUI, self.handleInput)
        return f

    def setUI(self, ui):
        self.ui = ui

    def handleInput(self, line):
        log.msg("Publishing line: %s" % (line,))
        self.publisher.publish_message(Message(message=line))

    def consume_message(self, message):
        log.msg("Consumed Message %s" % message)
        dictionary = message.payload
        src = dictionary.get('recipient', u'').encode('ascii')
        text = dictionary.get('message',u'').encode('ascii')
        if self.ui:
            self.ui.addOutputMessage("[%s]: %s" % (src, text))

    def stopWorker(self):
        log.msg("Stopping the console transport")
        if self.ui:
            self.ui.quit()
