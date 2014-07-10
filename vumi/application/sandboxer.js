var vm = require('vm');
var events = require('events');
var EventEmitter = events.EventEmitter;


var SandboxApi = function () {
    // API for use by applications
    var self = this;

    self.id = 0;
    self.emitter = new EventEmitter();

    self.next_id = function () {
        self.id += 1;
        return self.id.toString();
    };

    self.populate_command = function (command, msg) {
        msg.cmd = command;
        msg.reply = false;
        msg.cmd_id = self.next_id();
        return msg;
    };

    self.request = function (command, msg, callback) {
        // callback is optional and is called once a reply to
        // the request is received.
        self.populate_command(command, msg);
        self.emitter.emit('request', {
            msg: msg,
            callback: callback
        });
    };

    self.log_info = function (msg, callback) {
        self.request('log.info', {msg: msg}, callback);
    };

    self.done = function () {
        self.log_info('Done.', function() {
            self.emitter.emit('done');
        });
    };

    // handlers:
    // * on_unknown_command is the default message handler
    // * other handlers are looked up based on the command name
    self.on_unknown_command = function(command) {};
};

var SandboxRunner = function (api) {
    // Runner for a sandboxed app
    var self = this;
    self.emitter = new EventEmitter();

    self.api = api;
    self.chunk = "";
    self.pending_requests = {};
    self.loaded = false;

    self.emitter.on('command', function (command) {
        var handler_name = "on_" + command.cmd.replace('.', '_').replace('-', '_');
        var handler = api[handler_name];
        if (!handler) {
            handler = api.on_unknown_command;
        }
        if (handler) {
            handler.call(self.api, command);
        }
    });

    self.emitter.on('reply', function (reply) {
        var handler = self.pending_requests[reply.cmd_id];
        if (handler && handler.callback) {
            handler.callback.call(self.api, reply);
        }
    });

    self.api.emitter.on('request', function(request) {
        setImmediate(function() {
            if (request.callback) {
                self.pending_requests[request.msg.cmd_id] = {
                    callback: request.callback
                };
            }

            self.send_command(request.msg);
        });
    });

    self.api.emitter.on('done', function() {
        self.exit();
    });

    self.exit = function() {
        process.exit(0);
    };

    self.load_code = function (command) {
        self.log("Loading sandboxed code ...");
        var ctxt;
        var loaded_module = vm.createScript(command.javascript);
        if (command.app_context) {
            // TODO use vm stuff instead of eval
            eval("ctxt = " + command.app_context + ";");  // jshint ignore:line
        } else {
            ctxt = {};
        }
        ctxt.api = self.api;
        loaded_module.runInNewContext(ctxt);
        self.loaded = true;
    };

    self.send_command = function (cmd) {
        process.stdout.write(JSON.stringify(cmd));
        process.stdout.write("\n");
    };

    self.log = function(msg) {
        var cmd = self.api.populate_command("log.info", {"msg": msg});
        self.send_command(cmd);
    };

    self.data_from_stdin = function (data) {
        var parts = data.split("\n");
        parts[0] = self.chunk + parts[0];
        for (i = 0; i < parts.length - 1; i++) {
            if (!parts[i]) {
                continue;
            }
            var msg = JSON.parse(parts[i]);
            if (!self.loaded) {
                if (msg.cmd == 'initialize') {
                    self.load_code(msg);
                }
            }
            else if (!msg.reply) {
                self.emitter.emit('command', msg);
            }
            else {
                self.emitter.emit('reply', msg);
            }
        }
        self.chunk = parts[parts.length - 1];
    };

    self.run = function () {
        process.stdin.resume();
        process.stdin.setEncoding('ascii');
        process.stdin.on('data', function(data) {
            self.data_from_stdin(data); });
    };
};


var api = new SandboxApi();
var runner = new SandboxRunner(api);

runner.run();
runner.log("Starting sandbox ...");
