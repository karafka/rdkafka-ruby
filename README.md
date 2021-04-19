# Rdkafka

[![Build Status](https://appsignal.semaphoreci.com/badges/rdkafka-ruby/branches/master.svg?style=shields)](https://appsignal.semaphoreci.com/projects/rdkafka-ruby)
[![Gem Version](https://badge.fury.io/rb/rdkafka.svg)](https://badge.fury.io/rb/rdkafka)
[![Maintainability](https://api.codeclimate.com/v1/badges/ecb1765f81571cccdb0e/maintainability)](https://codeclimate.com/github/appsignal/rdkafka-ruby/maintainability)

The `rdkafka` gem is a modern Kafka client library for Ruby based on
[librdkafka](https://github.com/edenhill/librdkafka/).
It wraps the production-ready C client using the [ffi](https://github.com/ffi/ffi)
gem and targets Kafka 1.0+ and Ruby 2.4+.

`rdkafka` was written because we needed a reliable Ruby client for
Kafka that supports modern Kafka at [AppSignal](https://appsignal.com).
We run it in production on very high traffic systems.

This gem only provides a high-level Kafka consumer. If you are running
an older version of Kafka and/or need the legacy simple consumer we
suggest using the [Hermann](https://github.com/reiseburo/hermann) gem.

The most important pieces of a Kafka client are implemented. We're
working towards feature completeness, you can track that here:
https://github.com/appsignal/rdkafka-ruby/milestone/1

# TODO

- Setup SSL for kafka cluster running on docker using a script to create certs during build.
- Consume and Produce messages using secure advertised listener. 
- Enable SASL_SSL authentication.

## Installation

This gem downloads and compiles librdkafka when it is installed. If you
have any problems installing the gem please open an issue.

## Usage

See the [documentation](https://www.rubydoc.info/github/appsignal/rdkafka-ruby) for full details on how to use this gem. Two quick examples:

### Consuming messages

Subscribe to a topic and get messages. Kafka will automatically spread
the available partitions over consumers with the same group id.

```ruby
config = {
  :"bootstrap.servers" => "localhost:9092",
  :"group.id" => "ruby-test"
}
consumer = Rdkafka::Config.new(config).consumer
consumer.subscribe("ruby-test-topic")

consumer.each do |message|
  puts "Message received: #{message}"
end
```

### Producing messages

Produce a number of messages, put the delivery handles in an array and
wait for them before exiting. This way the messages will be batched and
sent to Kafka in an efficient way.

```ruby
config = {:"bootstrap.servers" => "localhost:9092"}
producer = Rdkafka::Config.new(config).producer
delivery_handles = []

100.times do |i|
  puts "Producing message #{i}"
  delivery_handles << producer.produce(
      topic:   "ruby-test-topic",
      payload: "Payload #{i}",
      key:     "Key #{i}"
  )
end

delivery_handles.each(&:wait)
```

Note that creating a producer consumes some resources that will not be
released until it `#close` is explicitly called, so be sure to call
`Config#producer` only as necessary.

## Development

### SASL_SSL

#### Requirements

- Java Runtime or Development kit(to create java keystores for kafka)
- openssl (to generate ssl certs)

### Usage

Docker compose file: scripts/docker-compose-sasl.yml can be used to run kafka 
and zookeeper on SASL_SSL listeners.

This docker-compose file includes a service for kerberos (KDC server) as well.
To be able to make things work with kerberos, we need to have kerberos principals
and keytabs created before we start zookeeper or kafka. This is being managed as
part of script `scripts/start.sh`. This script includes below steps:

- Creates SSL certificates and Java Keystores needed by kafka.
- Generates Kerberos principals and keytabs
- Starts kafka and zookeeper. 

#### Starting kafka cluster

- Before running docker-compose you need to run below commands to build kerberos
    - `cd scripts/kdc-admin`
    - `docker build -t confluentinc/cp-kerberos:latest .`
- Execute below commands to start kafka cluster now:
    - `cd scripts`
    - `./start.sh`

### Testing SASL_SSL

Rakefile in this project relies on Environment variables for running tasks
called 'produce_messages' and 'consume_messages". Export below environment
variables before running these tests:

- `export BOOTSTRAP_SERVERS, SASL_SSL, PRODUCER_KEYTAB, PRODUCER_PRINCIPAL,
    SSL_CA_LOCATION, CONSUMER_KEYTAB, CONSUMER_PRINCIPAL`

- Examples:
    - `export BOOTSTRAP_SERVERS=kafka.confluent.io:9094`
      `Note:` exporting BOOTSTRAP_SERVERS will be required to test PLAINTEXT as well
    - `export SASL_SSL=True`
    - `export PRODUCER_KEYTAB=scripts/secrets/producer.keytab`
    - `export PRODUCER_PRINCIPAL=producer/producer.confluent.io@TEST.CONFLUENT.IO`
    - `export CONSUMER_KEYTAB=scripts/secrets/consumer.keytab`
    - `export CONSUMER_PRINCIPAL=consumer/consumer.confluent.io@TEST.CONFLUENT.IO`
    - `export SSL_CA_LOCATION=scripts/secrets/ca.crt`


### Old

A Docker Compose file is included to run Kafka and Zookeeper. To run
that:

```
docker-compose up
```

Run `bundle` and `cd ext && bundle exec rake && cd ..` to download and
compile `librdkafka`.

You can then run `bundle exec rspec` to run the tests. To see rdkafka
debug output:

```
DEBUG_PRODUCER=true bundle exec rspec
DEBUG_CONSUMER=true bundle exec rspec
```

After running the tests you can bring the cluster down to start with a
clean slate:

```
docker-compose down
```

## Example

To see everything working run these in separate tabs:

```
bundle exec rake consume_messages
bundle exec rake produce_messages
```
