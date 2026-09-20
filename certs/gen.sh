#!/usr/bin/env bash
# Self-signed CA + server cert for the TLS test container, plus a client cert (signed by the same CA) for
# the mTLS test container. Regenerated on demand; outputs are gitignored.
set -euo pipefail
cd "$(dirname "$0")"

if [[ -f ca.crt && -f redis.crt && -f redis.key ]]; then
  echo "certs/ca.crt, certs/redis.crt, certs/redis.key already present, skipping"
else
  if [[ -f ca.crt && ! -f ca.key ]]; then
    echo "certs/ca.crt exists but certs/ca.key is missing: cannot safely regenerate the CA without invalidating" >&2
    echo "certificates already derived from it. Restore ca.key, or remove ca.crt (and any client/redis certs) to" >&2
    echo "start over with a fresh CA." >&2
    exit 1
  fi
  rm -f client.key client.crt client.pfx   # signed by the CA this replaces
  openssl genrsa -out ca.key 2048 2>/dev/null
  openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -subj "/CN=redis-near-cache test CA" -out ca.crt
  openssl genrsa -out redis.key 2048 2>/dev/null
  openssl req -new -key redis.key -subj "/CN=localhost" -out redis.csr
  printf "subjectAltName=DNS:localhost,DNS:redis-tls,IP:127.0.0.1\n" > san.ext
  openssl x509 -req -in redis.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 3650 -sha256 -extfile san.ext -out redis.crt 2>/dev/null
  chmod 644 redis.key ca.key   # the container runs redis as a non-root user and must read the key
  rm -f redis.csr san.ext ca.srl
  echo "generated certs/ca.crt certs/redis.crt certs/redis.key"
fi

if [[ -f client.key && -f client.crt && -f client.pfx ]]; then
  echo "certs/client.key, certs/client.crt, certs/client.pfx already present, skipping"
  exit 0
fi

if [[ ! -f ca.key ]]; then
  echo "certs/ca.key is missing: cannot sign a new client certificate. Restore ca.key, or remove ca.crt/redis.*" >&2
  echo "so a fresh CA (and client cert) can be generated." >&2
  exit 1
fi

openssl genrsa -out client.key 2048 2>/dev/null
openssl req -new -key client.key -subj "/CN=rnc-test-client" -out client.csr
printf "extendedKeyUsage=clientAuth\n" > client.ext
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 3650 -sha256 -extfile client.ext -out client.crt 2>/dev/null
chmod 644 client.key   # the container runs redis as a non-root user and must read the key
rm -f client.csr client.ext ca.srl

# Password is fixed (rnc-test) so integration tests can hard-code it. No -legacy flag: on OpenSSL 3 that
# yields the modern (AES) PKCS#12 encryption, which .NET reads fine on both macOS and Linux; on older
# OpenSSL/LibreSSL the default (RC2/3DES) is what .NET has always read fine too.
openssl pkcs12 -export -in client.crt -inkey client.key -out client.pfx -passout pass:rnc-test -name rnc-test-client 2>/dev/null

echo "generated certs/client.key certs/client.crt certs/client.pfx"
