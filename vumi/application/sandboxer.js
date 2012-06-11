var fs = require('fs');
var vm = require('vm');
var events = require('events');


var SandboxApi = function () {
    // API for use by applications
    var self = this;
    self.id = 0;
    self.waiting_requests = [];

    self.next_id = function () {
        self.id += 1;
        return self.id;
    }

    self.populate_command = function (command, msg) {
        msg.cmd = command;
        msg.reply = false;
        msg.cmd_id = self.next_id();
        return msg;
    }

    self.request = function (command, msg, callback) {
        // * callback is optional and is called once a reply to
        //   the request is received.
        // * requests created are only sent once the handler
        //   completes.
        // * callbacks can generate more requests.
        self.populate_command(command, msg);
        if (callback) {
            msg._callback = callback;
        }
        self.waiting_requests.push(msg);
    }

    self.pop_requests = function () {
        var requests = self.waiting_requests.splice(0,
            self.waiting_requests.length);
        return requests;
    }

    self.log_info = function (msg, callback) {
        self.request('log.info', {'msg': msg}, callback);
    }

    self.done = function () {
        self.request('log.info', {'_last': true, 'msg': "Done."});
    }
}

var SandboxRunner = function (api, ctx) {
    // Runner for a sandboxed app
    var self = this;

    self.api = api;
    self.ctx = ctx;
    self.emitter = new events.EventEmitter();
    self.chunk = "";
    self.pending_requests = {};

    self.emitter.on('init', function () {
        self.ctx.on_init.call(self.ctx, self.api);
        self.process_requests(api.pop_requests());
    });

    self.emitter.on('command', function (command) {
        var handler_name = "on_" + command.cmd.replace('.', '_').replace('-', '_');
        var handler = ctx[handler_name];
        if (handler === undefined) {
            handler = ctx['on_unknown'];
        }
        if (handler !== undefined) {
            handler.call(self.ctx, self.api, command);
            self.process_requests(api.pop_requests());
        }
    });

    self.emitter.on('reply', function (reply) {
        var handler = self.pending_requests[reply.msg_id];
        if (handler === undefined) {
            return;
        }
        if (handler.callback) {
            handler.callback.call(ctx, reply);
            self.process_requests(api.pop_requests());
            return;
        }
    });

    self.emitter.on('exit', function () {
        process.exit(0);
    });

    self.process_requests = function (requests) {
        requests.forEach(function (msg) {
            var last = msg._last;
            delete msg._last;
            var callback = msg._callback;
            delete msg._callback;
            self.send_command(msg);
            if (last) {
                self.emitter.emit('exit');
            }
            if (callback) {
                self.pending_requests[msg.msg_id] = {'callback': callback};
            }
        });
    }

    self.send_command = function (cmd) {
        process.stdout.write(JSON.stringify(cmd));
        process.stdout.write("\n");
    }

    self.log = function(msg) {
        var cmd = self.api.populate_command("log.info", {"msg": msg});
        self.send_command(cmd);
    }

    self.data_from_stdin = function (data) {
        parts = data.split("\n");
        parts[0] = self.chunk + parts[0];
        for (i = 0; i < parts.length - 1; i++) {
            if (!parts[i]) {
                continue;
            }
            msg = JSON.parse(parts[i]);
            if (!msg.reply) {
                self.emitter.emit('command', msg);
            }
            else {
                self.emitter.emit('reply', msg);
            }
        }
        self.chunk = parts[parts.length - 1];
    }

    self.run = function () {
        self.emitter.emit('init');
        process.stdin.resume();
        process.stdin.setEncoding('ascii');
        process.stdin.on('data', function(data) {
            self.data_from_stdin(data); });
    }
};


var api = new SandboxApi();
var ctx = {};
var runner = new SandboxRunner(api, ctx);

runner.log("Loading sandboxed code ...");
var data = fs.readFileSync(process.argv[2]);
var loaded_module = vm.createScript(data);
loaded_module.runInNewContext(ctx);

runner.log("Starting sandbox ...");
runner.run();

runner.log("Sandbox running ...");
