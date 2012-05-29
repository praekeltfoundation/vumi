// Demonstration App

function on_init(api) {
    api.send_command({"cmd": "log.info", "msg": "From init!"});
}

function on_command(api, command) {
    api.send_command({
        "cmd": "log.info",
        "msg": "From command: " + command.cmd
        });
    if (command.msg) {
        api.done();
    }
}
