# Modern Kafka client library for Ruby based on librdkafka

Kafka client library wrapping librdkafka using the ffi gem and futures
from concurrent-ruby for Kafka 0.10+ and Ruby 2.1+".

## Development

Run `bundle` and `cd ext && bundle exec rake compile && cd ..`. You can then run
`bundle exec rspec` to run the tests.
