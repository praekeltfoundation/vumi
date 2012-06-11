// Demonstration App

function on_init(api) {
    // Called when sandbox is initialized.
    // Guaranteed to be called before any messages are received.
    api.log_info("From init!");
}

function on_unknown(api, command) {
    // Called for any command that doesn't have an explicit
    // command handler.
    api.log_info("From unknown: " + command.cmd);
}

// Command handlers

function on_initialize(api, command) {
    api.log_info("From command: initialize");
}

function on_inbound_message(api, command) {
    api.log_info("From command: inbound-message", function (reply) {
        api.log_info("Log successful: " + reply.success);
        api.done();
    });
}
