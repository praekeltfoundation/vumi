define rabbitmq::plugin($ensure) {
    exec { "Install $name-$version rabbitmq plugin":
        command => "/usr/sbin/rabbitmqctl add_vhost $name",
        user => "root",
        unless => "/usr/sbin/rabbitmqctl list_vhosts | grep $name"
    }
}
