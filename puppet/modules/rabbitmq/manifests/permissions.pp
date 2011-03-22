define rabbitmq::permissions(
    $ensure = 'present',
    $vhost = '/',
    $conf = '.*',
    $read = '.*',
    $write = '.*'
) {
    case $ensure {
        present: {
            exec { "Set permissions $conf $read $write on $vhost for rabbitmq user $name":
                command => "/usr/sbin/rabbitmqctl set_permissions -p '$vhost' '$name' '$conf' '$read' '$write'",
                user => "root",
                unless => "/usr/sbin/rabbitmqctl list_user_permissions $name | grep $vhost",
            }
        }
        absent:  {
            exec { "Delete permissions on $name for rabbitmq user $user":
                command => "/usr/sbin/rabbitmqctl clear_permissions -p $vhost $name",
                user => "root",
                onlyif => "/usr/sbin/rabbitmqctl list_user_permissions $name | grep $vhost"
            }
        }
        default: {
            fail "Invalid 'ensure' value '$ensure' for rabbitmq::permissions"
        }
    }
}
