#!/bin/bash -e

TWISTD_APPLICATION="${TWISTD_APPLICATION:-vumi_worker}"

WORKER_CLASS_OPT=""
if [ -n "$WORKER_CLASS" ]; do
  WORKER_CLASS_OPT="--worker-class $WORKER_CLASS"
done

CONFIG_OPT=""
if [ -n "$CONFIG_FILE" ]; do
  CONFIG_OPT="--config $CONFIG_FILE"
done

AMQP_OPTS=""
if [ -n "$AMQP_HOST" ]; do
  AMQP_OPTS="--hostname $AMQP_HOST \
    --port ${AMQP_PORT:-5672} \
    --vhost ${AMQP_VHOST:-/} \
    --username ${AMQP_USERNAME:-guest} \
    --password ${AMQP_PASSWORD:-guest}"
done

SENTRY_OPT=""
if [ -n "$SENTRY_DSN" ]; do
  SENTRY_OPT="--sentry $SENTRY_DSN"
done

SET_OPTS=$(env | grep ^VUMI_OPT_ | sed -e 's/^VUMI_OPT_//' -e 's/=/ /' | awk '{printf("%s=%s:%s ", "--set-option", tolower($1), $2);}')

exec twistd --nodaemon \
  "$TWISTD_APPLICATION" \
  "$WORKER_CLASS_OPT" \
  "$CONFIG_OPT" \
  "$AMQP_OPTS" \
  "$SENTRY_OPT" \
  "$SET_OPTS" \
  "$@"
