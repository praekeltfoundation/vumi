FROM praekeltfoundation/python-base
MAINTAINER Praekelt Foundation <dev@praekeltfoundation.org>

ENV VUMI_VERSION "0.6.18"
RUN pip install vumi==$VUMI_VERSION

COPY ./vumi-entrypoint.sh /app/vumi-entrypoint.sh
WORKDIR /app

CMD ["/app/vumi-entrypoint.sh"]
