# Rdkafka

[![Build Status](https://github.com/karafka/rdkafka-ruby/actions/workflows/ci.yml/badge.svg)](https://github.com/karafka/rdkafka-ruby/actions/workflows/ci.yml)
[![Gem Version](https://badge.fury.io/rb/rdkafka.svg)](https://badge.fury.io/rb/rdkafka)
[![Join the chat at https://slack.karafka.io](https://raw.githubusercontent.com/karafka/misc/master/slack.svg)](https://slack.karafka.io)

> [!NOTE]
> The `rdkafka-ruby` gem was created and developed by [AppSignal](https://www.appsignal.com/). Their impactful contributions have significantly shaped the Ruby Kafka and Karafka ecosystems. For robust monitoring, we highly recommend AppSignal.

---

The `rdkafka` gem is a modern Kafka client library for Ruby based on
[librdkafka](https://github.com/confluentinc/librdkafka/).
It wraps the production-ready C client using the [ffi](https://github.com/ffi/ffi)
gem and targets Kafka 1.0+ and Ruby versions under security or
active maintenance. We remove a Ruby version from our CI builds when they 
become EOL.

`rdkafka` was written because of the need for a reliable Ruby client for Kafka that supports modern Kafka at [AppSignal](https://appsignal.com). AppSignal runs it in production on very high-traffic systems.

The most important pieces of a Kafka client are implemented, and we aim to provide all relevant consumer, producer, and admin APIs.

## Table of content

- [Project Scope](#project-scope)
- [Installation](#installation)
- [Usage](#usage)
  * [Consuming Messages](#consuming-messages)
  * [Producing Messages](#producing-messages)
- [Higher Level Libraries](#higher-level-libraries)
  * [Message Processing Frameworks](#message-processing-frameworks)
  * [Message Publishing Libraries](#message-publishing-libraries)
- [Development](#development)
- [Example](#example)

## Project Scope

While rdkafka-ruby aims to simplify the use of librdkafka in Ruby applications, it's important to understand the limitations of this library:

- **No Complex Producers/Consumers**: This library does not intend to offer complex producers or consumers. The aim is to stick closely to the functionalities provided by librdkafka itself.

- **Focus on librdkafka Capabilities**: Features that can be achieved directly in Ruby, without specific needs from librdkafka, are outside the scope of this library.

- **Existing High-Level Functionalities**: Certain high-level functionalities like producer metadata cache and simple consumer are already part of the library. Although they fall slightly outside the primary goal, they will remain part of the contract, given their existing usage.


## Installation

This gem downloads and compiles librdkafka when it is installed. If you
If you have any problems installing the gem, please open an issue.

## Usage

See the [documentation](https://karafka.io/docs/code/rdkafka-ruby/) for full details on how to use this gem. Two quick examples:

### Consuming Messages

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

### Producing Messages

Produce a number of messages, put the delivery handles in an array, and
wait for them before exiting. This way the messages will be batched and
efficiently sent to Kafka.

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

## Higher Level Libraries

Currently, there are two actively developed frameworks based on rdkafka-ruby, that provide higher-level API that can be used to work with Kafka messages and one library for publishing messages.

### Message Processing Frameworks

* [Karafka](https://github.com/karafka/karafka) - Ruby and Rails efficient Kafka processing framework.
* [Racecar](https://github.com/zendesk/racecar) - A simple framework for Kafka consumers in Ruby 

### Message Publishing Libraries

* [WaterDrop](https://github.com/karafka/waterdrop) – Standalone Karafka library for producing Kafka messages.

## Development

Contributors are encouraged to focus on enhancements that align with the core goal of the library. We appreciate contributions but will likely not accept pull requests for features that:

- Implement functionalities that can achieved using standard Ruby capabilities without changes to the underlying rdkafka-ruby bindings.
- Deviate significantly from the primary aim of providing librdkafka bindings with Ruby-friendly interfaces.

A Docker Compose file is included to run Kafka. To run that:

```
docker-compose up
```

Run `bundle` and `cd ext && bundle exec rake && cd ..` to download and compile `librdkafka`.

You can then run `bundle exec rspec` to run the tests. To see rdkafka debug output:

```
DEBUG_PRODUCER=true bundle exec rspec
DEBUG_CONSUMER=true bundle exec rspec
```

After running the tests, you can bring the cluster down to start with a clean slate:

```
docker-compose down
```

## Example

To see everything working, run these in separate tabs:

```
bundle exec rake consume_messages
bundle exec rake produce_messages
```
