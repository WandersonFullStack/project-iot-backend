#!/bin/sh
set -eu

: "${MQTT_USERNAME:?MQTT_USERNAME is required}"
: "${MQTT_PASSWORD:?MQTT_PASSWORD is required}"

CERT_HOST="${MQTT_CERT_HOST:-mosquitto}"
CERT_DIR="/mosquitto/certs"
DATA_DIR="/mosquitto/data"
HOST_MARKER="${CERT_DIR}/server-host"

case "$CERT_HOST" in
    ""|*[!A-Za-z0-9.-]*)
        echo "MQTT_CERT_HOST must be a valid DNS hostname" >&2
        exit 1
        ;;
esac

mkdir -p "$CERT_DIR" "$DATA_DIR"
umask 077

if [ ! -s "${CERT_DIR}/ca.crt" ] || [ ! -s "${CERT_DIR}/ca.key" ]; then
    echo "Generating private MQTT certificate authority..."
    rm -f "${CERT_DIR}/ca.crt" "${CERT_DIR}/ca.key"
    openssl req \
        -x509 \
        -newkey rsa:4096 \
        -sha256 \
        -nodes \
        -days 3650 \
        -keyout "${CERT_DIR}/ca.key" \
        -out "${CERT_DIR}/ca.crt" \
        -subj "/CN=IoT MQTT Private CA"
fi

RENEW_SERVER_CERT=false
if [ ! -s "${CERT_DIR}/server.crt" ] || [ ! -s "${CERT_DIR}/server.key" ]; then
    RENEW_SERVER_CERT=true
elif [ ! -s "$HOST_MARKER" ] || [ "$(cat "$HOST_MARKER")" != "$CERT_HOST" ]; then
    RENEW_SERVER_CERT=true
elif ! openssl x509 -checkend 2592000 -noout -in "${CERT_DIR}/server.crt"; then
    RENEW_SERVER_CERT=true
fi

if [ "$RENEW_SERVER_CERT" = true ]; then
    echo "Generating MQTT server certificate for ${CERT_HOST}..."

    if [ "$CERT_HOST" = "mosquitto" ]; then
        SUBJECT_ALT_NAMES="DNS:mosquitto"
    else
        SUBJECT_ALT_NAMES="DNS:mosquitto,DNS:${CERT_HOST}"
    fi

    openssl req \
        -new \
        -newkey rsa:2048 \
        -sha256 \
        -nodes \
        -keyout "${CERT_DIR}/server.key" \
        -out "${CERT_DIR}/server.csr" \
        -subj "/CN=${CERT_HOST}"

    {
        echo "basicConstraints=critical,CA:FALSE"
        echo "keyUsage=critical,digitalSignature,keyEncipherment"
        echo "extendedKeyUsage=serverAuth"
        echo "subjectAltName=${SUBJECT_ALT_NAMES}"
    } > "${CERT_DIR}/server.ext"

    openssl x509 \
        -req \
        -sha256 \
        -days 397 \
        -in "${CERT_DIR}/server.csr" \
        -CA "${CERT_DIR}/ca.crt" \
        -CAkey "${CERT_DIR}/ca.key" \
        -CAcreateserial \
        -out "${CERT_DIR}/server.crt" \
        -extfile "${CERT_DIR}/server.ext"

    printf '%s' "$CERT_HOST" > "$HOST_MARKER"
    rm -f "${CERT_DIR}/server.csr" "${CERT_DIR}/server.ext"
fi

echo "Updating MQTT credentials..."
rm -f "${DATA_DIR}/passwords"
mosquitto_passwd -b -c "${DATA_DIR}/passwords" "$MQTT_USERNAME" "$MQTT_PASSWORD"

chmod 0600 "${CERT_DIR}/ca.key" "${CERT_DIR}/server.key" "${DATA_DIR}/passwords"
chmod 0644 "${CERT_DIR}/ca.crt" "${CERT_DIR}/server.crt"
chown -R mosquitto:mosquitto "$CERT_DIR" "$DATA_DIR"

exec /docker-entrypoint.sh "$@"
