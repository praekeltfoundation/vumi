
rabbitmqctl add_user vumi vumi

rabbitmqctl add_vhost /development
rabbitmqctl set_permissions -p /development vumi '.*' '.*' '.*'

rabbitmqctl add_vhost /staging
rabbitmqctl set_permissions -p /staging vumi '.*' '.*' '.*'

rabbitmqctl add_vhost /production
rabbitmqctl set_permissions -p /production vumi '.*' '.*' '.*'


