// Demonstration App

api.log_info("From init!");

api.on_inbound_message = function(command) {
    if (path) {
        this.log_info("We have access to path!");
    } else {
        this.log_info("We don't have access to path. :(");
    }
    this.done();
}
