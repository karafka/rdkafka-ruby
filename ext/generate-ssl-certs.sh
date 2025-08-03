#!/bin/bash

#==============================================================================
# Kafka SSL Certificate Generator
#==============================================================================
#
# DESCRIPTION:
#   Generates SSL certificates for testing Kafka with SSL/TLS encryption.
#   Creates both Java KeyStore (JKS) files for Kafka server and PEM files
#   for client applications like rdkafka.
#
# PURPOSE:
#   - Test SSL connectivity between Kafka clients and brokers
#   - Validate rdkafka SSL integration
#   - Enable encrypted communication for development/testing environments
#
# USAGE:
#   ./ext/generate-ssl-certs.sh
#   docker compose -f docker-compose-ssl.yml up
#
# REQUIREMENTS:
#   - OpenSSL (for certificate generation)
#   - Java keytool (usually included with JDK/JRE)
#   - Write permissions in current directory
#
# OUTPUT FILES (created in ./ssl/ directory):
#   ├── kafka.server.keystore.jks    # Kafka server's private key and certificate
#   ├── kafka.server.truststore.jks  # Trusted CA certificates for Kafka
#   ├── kafka_keystore_creds         # Password file for keystore
#   ├── kafka_truststore_creds       # Password file for truststore
#   ├── kafka_ssl_key_creds          # Password file for SSL keys
#   ├── ca-cert                      # CA certificate (for rdkafka clients)
#   └── ca-cert.pem                  # CA certificate in PEM format
#
# CONFIGURATION:
#   - Certificate validity: 365 days
#   - Password: "confluent" (all certificates use same password for simplicity)
#   - Subject: CN=localhost (suitable for local testing)
#   - CA Subject: CN=localhost-ca
#
# DOCKER COMPOSE INTEGRATION:
#   Use with docker-compose-ssl.yml that mounts ./ssl directory to
#   /etc/kafka/secrets inside the Kafka container.
#
# RDKAFKA CLIENT CONFIGURATION:
#   security.protocol=SSL
#   ssl.ca.location=./ssl/ca-cert
#   ssl.endpoint.identification.algorithm=none  # For localhost testing
#
# NOTES:
#   - Safe to run multiple times (cleans up existing files)
#   - Certificates are self-signed and suitable for testing only
#   - For production, use certificates signed by a trusted CA
#   - All passwords are set to "confluent" for simplicity
#
#==============================================================================

# Create ssl directory and clean up any existing files
mkdir -p ssl
cd ssl

# Clean up existing files
rm -f kafka.server.keystore.jks kafka.server.truststore.jks
rm -f kafka_keystore_creds kafka_truststore_creds kafka_ssl_key_creds
rm -f ca-key ca-cert cert-file cert-signed ca-cert.srl ca-cert.pem

echo "Cleaned up existing SSL files..."

# Set variables
VALIDITY_DAYS=365
PASSWORD="confluent"  # Use a simpler, well-known password
DNAME="CN=localhost,OU=Test,O=Test,L=Test,ST=Test,C=US"

# Create password files (all same password for simplicity)
echo "$PASSWORD" > kafka_keystore_creds
echo "$PASSWORD" > kafka_truststore_creds
echo "$PASSWORD" > kafka_ssl_key_creds

# Step 1: Generate CA key and certificate
openssl req -new -x509 -keyout ca-key -out ca-cert -days $VALIDITY_DAYS -subj "/CN=localhost-ca/OU=Test/O=Test/L=Test/S=Test/C=US" -passin pass:$PASSWORD -passout pass:$PASSWORD

# Step 2: Create truststore and import the CA certificate
keytool -keystore kafka.server.truststore.jks -alias CARoot -import -file ca-cert -storepass $PASSWORD -keypass $PASSWORD -noprompt

# Step 3: Create keystore
keytool -keystore kafka.server.keystore.jks -alias localhost -validity $VALIDITY_DAYS -genkey -keyalg RSA -dname "$DNAME" -storepass $PASSWORD -keypass $PASSWORD

# Step 4: Create certificate signing request
keytool -keystore kafka.server.keystore.jks -alias localhost -certreq -file cert-file -storepass $PASSWORD -keypass $PASSWORD

# Step 5: Sign the certificate with the CA
openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed -days $VALIDITY_DAYS -CAcreateserial -passin pass:$PASSWORD

# Step 6: Import CA certificate into keystore
keytool -keystore kafka.server.keystore.jks -alias CARoot -import -file ca-cert -storepass $PASSWORD -keypass $PASSWORD -noprompt

# Step 7: Import signed certificate into keystore
keytool -keystore kafka.server.keystore.jks -alias localhost -import -file cert-signed -storepass $PASSWORD -keypass $PASSWORD -noprompt

# Export CA certificate to PEM format for rdkafka
cp ca-cert ca-cert.pem

# Clean up intermediate files (but keep ca-cert.pem for rdkafka)
rm ca-key cert-file cert-signed

echo "SSL certificates generated successfully!"
echo "Password: $PASSWORD"
echo ""
echo "For rdkafka, use ca-cert.pem or ca-cert files"
