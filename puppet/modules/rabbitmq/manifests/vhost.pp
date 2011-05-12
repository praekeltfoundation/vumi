define rabbitmq::vhost($ensure) {
    case $ensure {
        present: {
            exec { "Create $name rabbitmq vhost":
                command => "/usr/sbin/rabbitmqctl add_vhost $name",
                user => "root",
                unless => "/usr/sbin/rabbitmqctl list_vhosts | grep $name"
            }
        }
        absent:  {
            exec { "Remove $name rabbitmq vhost":
                command => "/usr/sbin/rabbitmqctl delete_vhost $name",
                user => "root",
                onlyif => "/usr/sbin/rabbitmqctl list_vhosts | grep $name"
            }
        }
        default: {
            fail "Invalid 'ensure' value '$ensure' for rabbitmq::vhost"
        }
    }
}
