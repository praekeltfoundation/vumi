var fs = require('fs');
var vm = require('vm');
var events = require('events');

var SandboxApi = function(stream) {
    // API for use by applications

    this.stream = stream;

    this.log_info = function(msg) {
        this.send_command({'cmd': 'log.info', 'msg': msg})
    }

    this.send_command = function (cmd) {
        if (!cmd.cmd_id) {
            cmd.cmd_id = 1;  // TODO: set this to a UUID4 id hex value
        }
        if (!cmd.reply) {
            cmd.reply = false;
        }
        this.stream.write(JSON.stringify(cmd));
        this.stream.write("\n");
    }

    this.done = function () {
        this.log_info("Done.");
        process.exit(0);
    }
}

var SandboxRunner = function(api, ctx) {
    // Runner for a sandboxed app
    var self = this;

    self.api = api;
    self.ctx = ctx;
    self.emitter = new events.EventEmitter();
    self.chunk = "";

    self.emitter.on('init', function () {
        self.ctx.on_init(self.api);
    });

    self.emitter.on('command', function (command) {
        handler_name = "on_" + command.cmd.replace('.', '_').replace('-', '_');
        handler = ctx[handler_name];
        if (handler === undefined) {
            handler = ctx['on_unknown'];
        }
        if (handler !== undefined) {
            handler.call(ctx, self.api, command);
        }
    });

    self.emitter.on('reply', function (reply) {
    });

    self.data_from_stdin = function (data) {
        parts = data.split("\n");
        parts[0] = self.chunk + parts[0];
        for (i = 0; i < parts.length - 1; i++) {
            msg = JSON.parse(parts[i]);
            if (!msg.reply) {
                self.emitter.emit('command', msg);
            }
            else {
                self.emitter.emit('reply', msg);
            }
        }
        self.chunk = parts[-1];
    }

    self.run = function () {
        self.emitter.emit('init');
        process.stdin.resume();
        process.stdin.setEncoding('ascii');
        process.stdin.on('data', function(data) {
            self.data_from_stdin(data); });
    }
};

var api = new SandboxApi(process.stdout);

api.log_info("Loading sandboxed code ...");
var data = fs.readFileSync(process.argv[2]);
var loaded_module = vm.createScript(data);
var ctx = {};
loaded_module.runInNewContext(ctx);

api.log_info("Creating sandbox ...");
var runner = new SandboxRunner(api, ctx);

api.log_info("Starting sandbox ...");
runner.run();

api.log_info("Sandbox running ...");
