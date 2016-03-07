FROM praekeltfoundation/python-base
MAINTAINER Praekelt Foundation <dev@praekeltfoundation.org>

ENV VUMI_VERSION "0.6.2"
RUN pip install vumi==$VUMI_VERSION

COPY ./vumi-entrypoint.sh /app/vumi-entrypoint.sh
WORKDIR /app

ENTRYPOINT ["eval-args.sh", "dinit", "/app/vumi-entrypoint.sh"]
CMD []
