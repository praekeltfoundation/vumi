define rabbitmq::plugin($ensure) {
    exec { "Install $name-$ensure rabbitmq plugin":
        cwd => "/usr/lib/rabbitmq/lib/rabbitmq_server-$ensure/plugins",
        command => "wget -cN http://www.rabbitmq.com/releases/plugins/v$ensure/$name-$ensure.ez",
        user => "root"
    }
}
