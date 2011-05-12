define rabbitmq::user(
    $ensure = 'present', 
    $password = false,
    $vhost = '/',
    $conf = '.*',
    $read = '.*',
    $write = '.*'
) {
    case $ensure {
        present: {
            if ! $password {
                fail "Please provide a password when adding a user"
            }
            exec { "Create $name rabbitmq user":
                command => "/usr/sbin/rabbitmqctl add_user $name $password",
                user => "root",
                unless => "/usr/sbin/rabbitmqctl list_users | grep $name"
            }
        }
        absent:  {
            exec { "Remove $name rabbitmq user":
                command => "/usr/sbin/rabbitmqctl delete_user $name",
                user => "root",
                onlyif => "/usr/sbin/rabbitmqctl list_users | grep $name"
            }
        }
        default: {
            fail "Invalid 'ensure' value '$ensure' for rabbitmq::user"
        }
    }
    rabbitmq::permissions{$name:
        ensure => $ensure,
        vhost => $vhost,
        conf => $conf,
        read => $read,
        write => $write
    }
}
