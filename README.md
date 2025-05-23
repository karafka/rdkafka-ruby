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

The most essential pieces of a Kafka client are implemented, and we aim to provide all relevant consumer, producer, and admin APIs.

## Table of content

- [Project Scope](#project-scope)
- [Installation](#installation)
- [Usage](#usage)
  * [Consuming Messages](#consuming-messages)
  * [Producing Messages](#producing-messages)
- [Higher Level Libraries](#higher-level-libraries)
  * [Message Processing Frameworks](#message-processing-frameworks)
  * [Message Publishing Libraries](#message-publishing-libraries)
- [Forking](#forking)
- [Development](#development)
- [Example](#example)
- [Versions](#versions)

## Project Scope

While rdkafka-ruby aims to simplify the use of librdkafka in Ruby applications, it's important to understand the limitations of this library:

- **No Complex Producers/Consumers**: This library does not intend to offer complex producers or consumers. The aim is to stick closely to the functionalities provided by librdkafka itself.

- **Focus on librdkafka Capabilities**: Features that can be achieved directly in Ruby, without specific needs from librdkafka, are outside the scope of this library.

- **Existing High-Level Functionalities**: Certain high-level functionalities like producer metadata cache and simple consumer are already part of the library. Although they fall slightly outside the primary goal, they will remain part of the contract, given their existing usage.


## Installation

When installed, this gem downloads and compiles librdkafka. If you have any problems installing the gem, please open an issue.

## Usage

Please see the [documentation](https://karafka.io/docs/code/rdkafka-ruby/) for full details on how to use this gem. Below are two quick examples.

Unless you are seeking specific low-level capabilities, we **strongly** recommend using [Karafka](https://github.com/karafka/karafka) and [WaterDrop](https://github.com/karafka/waterdrop) when working with Kafka. These are higher-level libraries also maintained by us based on rdkafka-ruby.

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

Produce several messages, put the delivery handles in an array, and
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

Note that creating a producer consumes some resources that will not be released until it `#close` is explicitly called, so be sure to call `Config#producer` only as necessary.

## Higher Level Libraries

Currently, there are two actively developed frameworks based on `rdkafka-ruby`, that provide higher-level API that can be used to work with Kafka messages and one library for publishing messages.

### Message Processing Frameworks

* [Karafka](https://github.com/karafka/karafka) - Ruby and Rails efficient Kafka processing framework.
* [Racecar](https://github.com/zendesk/racecar) - A simple framework for Kafka consumers in Ruby 

### Message Publishing Libraries

* [WaterDrop](https://github.com/karafka/waterdrop) – Standalone Karafka library for producing Kafka messages.

## Forking

When working with `rdkafka-ruby`, it's essential to know that the underlying `librdkafka` library does not support fork-safe operations, even though it is thread-safe. Forking a process after initializing librdkafka clients can lead to unpredictable behavior due to inherited file descriptors and memory states. This limitation requires careful handling, especially in Ruby applications that rely on forking.

To address this, it's highly recommended to:

- Never initialize any `rdkafka-ruby` producers or consumers before forking to avoid state corruption.
- Before forking, always close any open producers or consumers if you've opened any.
- Use high-level libraries like [WaterDrop](https://github.com/karafka/waterdrop) and [Karafka](https://github.com/karafka/karafka/), which provide abstractions for handling librdkafka's intricacies.

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

## Versions

| rdkafka-ruby | librdkafka | patches |
|-|-|-|
| 0.19.x (2025-01-20) | 2.8.0 (2025-01-07)  | yes |
| 0.18.0 (2024-11-26) | 2.6.1 (2024-11-18)  | yes |
| 0.17.4 (2024-09-02) | 2.5.3 (2024-09-02)  | yes |
| 0.17.0 (2024-08-01) | 2.5.0 (2024-07-10)  | yes |
| 0.16.0 (2024-06-13) | 2.4.0 (2024-05-07)  | no  |
| 0.15.0 (2023-12-03) | 2.3.0 (2023-10-25)  | no  |
| 0.14.0 (2023-11-21) | 2.2.0 (2023-07-12)  | no  |
| 0.13.0 (2023-07-24) | 2.0.2 (2023-01-20)  | no  |
| 0.12.0 (2022-06-17) | 1.9.0 (2022-06-16)  | no  |
| 0.11.0 (2021-11-17) | 1.8.2 (2021-10-18)  | no  |
| 0.10.0 (2021-09-07) | 1.5.0 (2020-07-20)  | no  |
