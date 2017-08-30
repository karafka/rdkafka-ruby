# Modern Kafka client library for Ruby based on librdkafka

Kafka client library wrapping [librdkafka](https://github.com/edenhill/librdkafka/)
using the [ffi](https://github.com/ffi/ffi) gem for Kafka 0.10+ and Ruby 2.1+.

This gem only provides a high-level Kafka consumer. If you are running
an older version of Kafka and/or need the legacy simple consumer we
suggest using the [Hermann](https://github.com/reiseburo/hermann) gem.

## Development

Run `bundle` and `cd ext && bundle exec rake compile && cd ..`. Then
create the topics as expected in the specs: `bundle exec rake create_topics`.

You can then run `bundle exec rspec` to run the tests. To see rdkafka
debug output:

```
DEBUG_PRODUCER=true bundle exec rspec
DEBUG_CONSUMER=true bundle exec rspec
```

To see everything working run these in separate tabs:

```
bundle exec rake consume_messages
bundle exec rake produce_messages
```
