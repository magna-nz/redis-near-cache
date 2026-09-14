#!/usr/bin/env bash
# Self-signed CA + server cert for the TLS test container. Regenerated on demand; outputs are gitignored.
set -euo pipefail
cd "$(dirname "$0")"
if [[ -f ca.crt && -f redis.crt && -f redis.key ]]; then exit 0; fi
openssl genrsa -out ca.key 2048 2>/dev/null
openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -subj "/CN=redis-near-cache test CA" -out ca.crt
openssl genrsa -out redis.key 2048 2>/dev/null
openssl req -new -key redis.key -subj "/CN=localhost" -out redis.csr
printf "subjectAltName=DNS:localhost,DNS:redis-tls,IP:127.0.0.1\n" > san.ext
openssl x509 -req -in redis.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 3650 -sha256 -extfile san.ext -out redis.crt 2>/dev/null
chmod 644 redis.key ca.key   # the container runs redis as a non-root user and must read the key
rm -f redis.csr san.ext ca.srl
echo "generated certs/ca.crt certs/redis.crt certs/redis.key"
