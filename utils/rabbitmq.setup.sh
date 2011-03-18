
rabbitmqctl add_user vumi vumi

rabbitmqctl add_vhost /staging
rabbitmqctl set_permissions -p /staging vumi '.*' '.*' '.*'

rabbitmqctl add_vhost /production
rabbitmqctl set_permissions -p /production vumi '.*' '.*' '.*'

rabbitmqctl add_vhost /unstable
rabbitmqctl set_permissions -p /unstable vumi '.*' '.*' '.*'

