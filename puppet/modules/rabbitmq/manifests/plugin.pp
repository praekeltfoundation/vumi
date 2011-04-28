define rabbitmq::vhost($ensure) {
    case $ensure {
        present: {
            exec { "Install $name-$version rabbitmq plugin":
                command => "/usr/sbin/rabbitmqctl add_vhost $name",
                user => "root",
                unless => "/usr/sbin/rabbitmqctl list_vhosts | grep $name"
            }
        }
        default: {
            fail "Invalid 'ensure' value '$ensure' for rabbitmq::plugin"
        }
    }
}
