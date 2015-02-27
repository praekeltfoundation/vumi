// Demonstration App

api.log_info("From init!");

api.on_unknown_command = function(command) {
    setImmediate(function() {
        // Called for any command that doesn't have an explicit
        // command handler.
        api.log_info("From unknown: " + command.cmd);
    });
};

api.on_inbound_message = function(command) {
    setImmediate(function() {
        api.log_info("From command: inbound-message", function (reply) {
            api.log_info("Log successful: " + reply.success);
            api.done();
        });
    });
};
