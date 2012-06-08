// Demonstration App

function on_init(api) {
    // Called when sandbox is initialized.
    // Guaranteed to be called before any messages are received.
    api.send_command({
        "cmd": "log.info",
        "msg": "From init!"
        });
}

function on_unknown(api, command) {
    // Called for any command that doesn't have an explicit
    // command handler.
    api.send_command({
        "cmd": "log.info",
        "msg": "From unknown: " + command.cmd
        });
}

// Command handlers

function on_initialize(api, command) {
    api.send_command({
        "cmd": "log.info",
        "msg": "From command: " + command.cmd
        });
}

function on_inbound_message(api, command) {
    api.send_command({
        "cmd": "log.info",
        "msg": "From command: " + command.cmd
        });
    api.done();
}
