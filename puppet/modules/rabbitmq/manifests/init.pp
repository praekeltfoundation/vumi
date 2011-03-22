class rabbitmq {
	package { [rabbitmq-server,]: ensure => installed }

    service { rabbitmq-server:
        ensure => running,
        enable => true, # start at boot
        hasstatus => true, # has a working status command
        subscribe => [Package[rabbitmq-server],]
    }
}
