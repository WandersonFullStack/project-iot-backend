FROM eclipse-mosquitto:2

USER root

RUN apk add --no-cache openssl

COPY mosquitto.conf /mosquitto/config/mosquitto.conf
COPY docker-entrypoint-secure.sh /usr/local/bin/docker-entrypoint-secure

RUN chmod 0755 /usr/local/bin/docker-entrypoint-secure \
    && chown mosquitto:mosquitto /mosquitto/config/mosquitto.conf

ENTRYPOINT ["/usr/local/bin/docker-entrypoint-secure"]
CMD ["/usr/sbin/mosquitto", "-c", "/mosquitto/config/mosquitto.conf"]
