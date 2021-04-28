#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

# stop running docker-compose containers
docker-compose down --volumes --remove-orphans

# Create SSL Certificates and Java keystores
${DIR}/create-certs.sh

cd ${DIR}
echo "Starting kerberos container..."
docker-compose -f docker-compose-sasl.yml up --build -d kerberos

## Step-1
echo "Creating Service Principals..."
principals=(zookeeper kafka producer consumer)

for principal in ${principals[@]}; do
    # Delete existing keytabs before creating new ones
    docker exec -i kerberos rm -f /tmp/keytab/${principal}.keytab 2>&1 > /dev/null
    docker exec -i kerberos kadmin.local -w password -q "addprinc -randkey ${principal}/${principal}.confluent.io@TEST.CONFLUENT.IO"
    docker exec -i kerberos kadmin.local -w password -q "ktadd -norandkey -k /tmp/keytab/${principal}.keytab ${principal}/${principal}.confluent.io@TEST.CONFLUENT.IO"
    # keytabs are created on kerberos container with root user
    # ubi8 confluent images starting from version 6.x use 'appuser' user
    # adjusting keytab permissions
    docker exec -i kerberos chmod a+r /tmp/keytab/${principal}.keytab
done

# start zookeeper and kafka in background
docker-compose -f ${DIR}/docker-compose-sasl.yml up -d
