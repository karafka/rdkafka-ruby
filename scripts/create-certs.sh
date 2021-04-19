#!/bin/bash

set -o nounset \
    -o errexit \

# Clean up old certs from secrets directory only
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/secrets" >/dev/null && pwd )"
cd "${DIR}" && rm -f *.crt *.csr *_creds *.jks *.srl *.key *.pem *.der *.p12

cd "${DIR}"
# Generate CA Key and cert
openssl req -new -x509 -keyout ca.key -out ca.crt -days 365 \
    -subj '/CN=ca/OU=TEST/O=TEST/L=Toronto/S=ON/C=CA' \
    -passin pass:changeme -passout pass:changeme

for i in kafka.confluent.io
do
	echo $i
	# Create keystores
	keytool -genkey -noprompt \
				 -alias $i \
				 -dname "CN=$i, OU=TEST, O=TEST, L=Toronto, S=ON, C=CA" \
				 -keystore $i.keystore.jks \
                 -storetype PKCS12 \
				 -keyalg RSA \
				 -storepass changeme \
				 -keypass changeme 

	# Create CSR, sign the key and import back into keystore
	keytool -keystore $i.keystore.jks -alias $i -certreq -file $i.csr -storepass changeme -keypass changeme

	openssl x509 -req -CA ca.crt -CAkey ca.key -in $i.csr -out $i-ca-signed.crt -days 9999 -CAcreateserial -passin pass:changeme

	keytool -noprompt -keystore $i.keystore.jks -alias CARoot -import -file ca.crt -storepass changeme -keypass changeme

	keytool -noprompt -keystore $i.keystore.jks -alias $i -import -file $i-ca-signed.crt -storepass changeme -keypass changeme 

	# Create truststore and import the CA cert.
	keytool -noprompt -keystore $i.truststore.jks -alias CARoot -import -file ca.crt -storepass changeme -keypass changeme

  # Write jks creds to files needed by kafka-images/.../docker/configure
  echo "changeme" > ${i}_sslkey_creds
  echo "changeme" > ${i}_keystore_creds
  echo "changeme" > ${i}_truststore_creds
done

# Generating public and private keys for token signing
  echo "Generating public and private keys for token signing"
  mkdir -p keypair
  openssl genrsa -out keypair/keypair.pem 2048
  openssl rsa -in keypair/keypair.pem -outform PEM -pubout -out keypair/public.pem

  # Enable Docker appuser to read files when created by a different UID
  echo -e "Setting insecure permissions on some files in ${DIR}...\n"
  chmod 644 keypair/keypair.pem
  chmod 644 *.key
