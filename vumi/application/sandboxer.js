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

    this.api = api;
    this.emitter = new events.EventEmitter();
    this.chunk = "";

    if (ctx.on_init) {
        this.emitter.on('init', ctx.on_init);
    }

    if (ctx.on_command) {
        this.emitter.on('command', ctx.on_command);
    }

    this.data_from_stdin = function (data) {
        parts = data.split("\n");
        parts[0] = this.chunk + parts[0];
        for (i = 0; i < parts.length - 1; i++) {
            this.emitter.emit('command', this.api, JSON.parse(parts[i]));
        }
        this.chunk = parts[-1];
    }

    this.run = function () {
        var self = this;
        this.emitter.emit('init', this.api);
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
