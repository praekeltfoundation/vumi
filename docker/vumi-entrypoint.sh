#!/usr/bin/env bash
set -e

TWISTD_COMMAND="${TWISTD_COMMAND:-vumi_worker}"

WORKER_CLASS_OPT="${WORKER_CLASS:+--worker-class $WORKER_CLASS}"
CONFIG_OPT="${CONFIG_FILE:+--config $CONFIG_FILE}"

AMQP_OPTS=""
if [[ -n "$AMQP_HOST" ]]; then
  AMQP_OPTS="--hostname $AMQP_HOST \
    --port ${AMQP_PORT:-5672} \
    --vhost ${AMQP_VHOST:-/} \
    --username ${AMQP_USERNAME:-guest} \
    --password ${AMQP_PASSWORD:-guest}"
fi

SENTRY_OPT="${SENTRY_DSN:+--sentry $SENTRY_DSN}"

SET_OPTS=$(env | grep ^VUMI_OPT_ | sed -e 's/^VUMI_OPT_//' -e 's/=/ /' | awk '{printf("%s=%s:%s ", "--set-option", tolower($1), $2);}')

exec twistd --nodaemon \
  $TWISTD_COMMAND \
  $WORKER_CLASS_OPT \
  $CONFIG_OPT \
  $AMQP_OPTS \
  $SENTRY_OPT \
  $SET_OPTS \
  "$@"
