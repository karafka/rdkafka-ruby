# Modern Kafka client library for Ruby based on librdkafka

Kafka client library wrapping `librdkafka` using the FFI gem for Kafka 0.10+ and Ruby 2.1+".

## Development

Run `bundle` and `cd ext && bundle exec rake compile && cd ..`. Then
create the topics as expected in the specs: `bundle exec rake create_topics`.

You can then run `bundle exec rspec` to run the tests. To see rdkafka
debug output:

```
DEBUG_PRODUCER=true bundle exec rspec
DEBUG_CONSUMER=true bundle exec rspec
```
