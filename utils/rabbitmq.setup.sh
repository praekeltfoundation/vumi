#!/bin/bash
rabbitmqctl add_user vumi vumi
rabbitmqctl add_vhost /develop
rabbitmqctl set_permissions -p /develop vumi '.*' '.*' '.*'

