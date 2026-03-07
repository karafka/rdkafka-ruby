# frozen_string_literal: true

require "test_helper"
require "zlib"

class ProducerTest < Minitest::Test
  def producer
    @producer ||= rdkafka_producer_config.producer
  end

  def all_partitioners
    @all_partitioners ||= %w[random consistent consistent_random murmur2 murmur2_random fnv1a fnv1a_random]
  end

  def consumer
    @consumer ||= rdkafka_consumer_config.consumer
  end

  def teardown
    # Registry should always end up being empty
    registry = Rdkafka::Producer::DeliveryHandle::REGISTRY

    assert_empty registry, registry.inspect
    producer&.close
    consumer&.close
    super
  end

  # -- producer without auto-start --

  def test_producer_without_auto_start_can_start_later_and_close
    prod = rdkafka_producer_config.producer(native_kafka_auto_start: false)
    prod.start
    prod.close
  end

  def test_producer_without_auto_start_can_close_without_starting
    prod = rdkafka_producer_config.producer(native_kafka_auto_start: false)
    prod.close
  end

  # -- #name --

  def test_name
    assert_includes producer.name, "rdkafka#producer-"
  end

  # -- #produce with topic config alterations --

  def test_produce_with_invalid_topic_config_raises_error
    assert_raises(Rdkafka::Config::ConfigError) do
      producer.produce(topic: "test", payload: "", topic_config: { invalid: "invalid" })
    end
  end

  def test_produce_with_valid_topic_config_does_not_raise
    producer.produce(topic: "test", payload: "", topic_config: { acks: 1 }).wait
  end

  def test_produce_with_topic_config_alteration_changes_behavior
    prod = rdkafka_producer_config(
      "message.timeout.ms": 1_000_000,
      "bootstrap.servers": "127.0.0.1:9094"
    ).producer

    error = assert_raises(Rdkafka::RdkafkaError) do
      prod.produce(
        topic: "produce_config_test",
        payload: "test",
        topic_config: {
          "compression.type": "gzip",
          "message.timeout.ms": 1
        }
      ).wait
    end
    assert_match(/msg_timed_out/, error.message)
  ensure
    prod&.close
  end

  # -- delivery callback with proc/lambda --

  def test_delivery_callback_sets_lambda_callback
    producer.delivery_callback = lambda { |delivery_handle| }

    assert_respond_to producer.delivery_callback, :call
  end

  def test_delivery_callback_calls_lambda_when_message_delivered
    callback_called = false

    producer.delivery_callback = lambda do |report|
      refute_nil report
      assert_equal "label", report.label
      assert_equal 1, report.partition
      assert_operator report.offset, :>=, 0
      assert_equal TestTopics.produce_test_topic, report.topic_name
      callback_called = true
    end

    # Produce a message
    handle = producer.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload",
      key: "key",
      label: "label"
    )

    assert_equal "label", handle.label

    # Wait for it to be delivered
    handle.wait(max_wait_timeout_ms: 15_000)

    # Join the producer thread.
    producer.close

    # Callback should have been called
    assert callback_called
  end

  def test_delivery_callback_lambda_provides_handle
    callback_handle = nil

    producer.delivery_callback = lambda { |_, handle| callback_handle = handle }

    # Produce a message
    handle = producer.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload",
      key: "key"
    )

    # Wait for it to be delivered
    handle.wait(max_wait_timeout_ms: 15_000)

    # Join the producer thread.
    producer.close

    assert_same handle, callback_handle
  end

  # -- delivery callback with callable object --

  def test_delivery_callback_sets_callable_object
    callback = Class.new do
      def call(stats)
      end
    end
    producer.delivery_callback = callback.new

    assert_respond_to producer.delivery_callback, :call
  end

  def test_delivery_callback_calls_callable_object_when_message_delivered
    called_report = []
    callback = Class.new do
      def initialize(called_report)
        @called_report = called_report
      end

      def call(report)
        @called_report << report
      end
    end
    producer.delivery_callback = callback.new(called_report)

    # Produce a message
    handle = producer.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload",
      key: "key"
    )

    # Wait for it to be delivered
    handle.wait(max_wait_timeout_ms: 15_000)

    # Join the producer thread.
    producer.close

    # Callback should have been called
    refute_nil called_report.first
    assert_equal 1, called_report.first.partition
    assert_operator called_report.first.offset, :>=, 0
    assert_equal TestTopics.produce_test_topic, called_report.first.topic_name
  end

  def test_delivery_callback_callable_object_provides_handle
    callback_handles = []
    callback = Class.new do
      def initialize(callback_handles)
        @callback_handles = callback_handles
      end

      def call(_, handle)
        @callback_handles << handle
      end
    end
    producer.delivery_callback = callback.new(callback_handles)

    # Produce a message
    handle = producer.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload",
      key: "key"
    )

    # Wait for it to be delivered
    handle.wait(max_wait_timeout_ms: 15_000)

    # Join the producer thread.
    producer.close

    # Callback should have been called
    assert_same handle, callback_handles.first
  end

  def test_delivery_callback_does_not_accept_non_callable
    assert_raises(TypeError) do
      producer.delivery_callback = "a string"
    end
  end

  # -- basic produce tests --

  def test_requires_a_topic
    error = assert_raises(ArgumentError) do
      producer.produce(
        payload: "payload",
        key: "key"
      )
    end
    assert_match(/missing keyword: :?topic/, error.message)
  end

  def test_produces_a_message
    # Produce a message
    handle = producer.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload",
      key: "key",
      label: "label"
    )

    # Should be pending at first
    assert_predicate handle, :pending?
    assert_equal "label", handle.label

    # Check delivery handle and report
    report = handle.wait(max_wait_timeout_ms: 5_000)

    refute_predicate handle, :pending?
    refute_nil report
    assert_equal 1, report.partition
    assert_operator report.offset, :>=, 0
    assert_equal "label", report.label

    # Flush and close producer
    producer.flush
    producer.close

    # Consume message and verify its content
    message = wait_for_message(
      topic: TestTopics.produce_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    assert_equal 1, message.partition
    assert_equal "payload", message.payload
    assert_equal "key", message.key
    assert_in_delta Time.now, message.timestamp, 10
  end

  def test_produces_a_message_with_specified_partition
    # Produce a message
    handle = producer.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload partition",
      key: "key partition",
      partition: 1
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: TestTopics.produce_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    assert_equal 1, message.partition
    assert_equal "key partition", message.key
  end

  def test_produces_a_message_to_same_partition_with_similar_partition_key
    # Avoid partitioner collisions.
    while true
      key = ("a".."z").to_a.shuffle.take(10).join("")
      partition_key = ("a".."z").to_a.shuffle.take(10).join("")
      partition_count = producer.partition_count(TestTopics.partitioner_test_topic)
      break if (Zlib.crc32(key) % partition_count) != (Zlib.crc32(partition_key) % partition_count)
    end

    # Produce a message with key, partition_key and key + partition_key
    messages_config = [{ key: key }, { partition_key: partition_key }, { key: key, partition_key: partition_key }]

    messages = messages_config.map do |m|
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "payload partition",
        key: m[:key],
        partition_key: m[:partition_key]
      )
      report = handle.wait(max_wait_timeout_ms: 5_000)

      wait_for_message(
        topic: TestTopics.partitioner_test_topic,
        delivery_report: report
      )
    end

    refute_equal messages[0].partition, messages[2].partition
    assert_equal messages[1].partition, messages[2].partition
    assert_equal key, messages[0].key
    assert_nil messages[1].key
    assert_equal key, messages[2].key
  end

  def test_produces_a_message_with_empty_string_without_crashing
    messages_config = [{ key: "a", partition_key: "" }]

    messages = messages_config.map do |m|
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "payload partition",
        key: m[:key],
        partition_key: m[:partition_key]
      )
      report = handle.wait(max_wait_timeout_ms: 5_000)

      wait_for_message(
        topic: TestTopics.partitioner_test_topic,
        delivery_report: report
      )
    end

    assert_operator messages[0].partition, :>=, 0
    assert_equal "a", messages[0].key
  end

  def test_produces_a_message_with_utf8_encoding
    handle = producer.produce(
      topic: TestTopics.produce_test_topic,
      payload: "Τη γλώσσα μου έδωσαν ελληνική",
      key: "key utf8"
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: TestTopics.produce_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    assert_equal 1, message.partition
    assert_equal "Τη γλώσσα μου έδωσαν ελληνική", message.payload.force_encoding("utf-8")
    assert_equal "key utf8", message.key
  end

  def test_produces_a_message_to_non_existing_topic_with_key_and_partition_key
    new_topic = "it-#{SecureRandom.uuid}"

    handle = producer.produce(
      # Needs to be a new topic each time
      topic: new_topic,
      payload: "payload",
      key: "key",
      partition_key: "partition_key",
      label: "label"
    )

    # Should be pending at first
    assert_predicate handle, :pending?
    assert_equal "label", handle.label

    # Check delivery handle and report
    report = handle.wait(max_wait_timeout_ms: 5_000)

    refute_predicate handle, :pending?
    refute_nil report
    assert_equal 0, report.partition
    assert_operator report.offset, :>=, 0
    assert_equal "label", report.label

    # Flush and close producer
    producer.flush
    producer.close

    # Consume message and verify its content
    message = wait_for_message(
      topic: new_topic,
      delivery_report: report,
      consumer: consumer
    )

    assert_equal 0, message.partition
    assert_equal "payload", message.payload
    assert_equal "key", message.key
    assert_in_delta Time.now, message.timestamp, 10
  end

  # -- timestamp --

  def test_timestamp_raises_type_error_if_not_nil_integer_or_time
    assert_raises(TypeError) do
      producer.produce(
        topic: TestTopics.produce_test_topic,
        payload: "payload timestamp",
        key: "key timestamp",
        timestamp: "10101010"
      )
    end
  end

  def test_produces_a_message_with_integer_timestamp
    handle = producer.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload timestamp",
      key: "key timestamp",
      timestamp: 1505069646252
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: TestTopics.produce_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    assert_equal 2, message.partition
    assert_equal "key timestamp", message.key
    assert_equal Time.at(1505069646, 252_000), message.timestamp
  end

  def test_produces_a_message_with_time_timestamp
    handle = producer.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload timestamp",
      key: "key timestamp",
      timestamp: Time.at(1505069646, 353_000)
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: TestTopics.produce_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    assert_equal 2, message.partition
    assert_equal "key timestamp", message.key
    assert_equal Time.at(1505069646, 353_000), message.timestamp
  end

  # -- nil key / nil payload --

  def test_produces_a_message_with_nil_key
    handle = producer.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload no key"
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: TestTopics.produce_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    assert_nil message.key
    assert_equal "payload no key", message.payload
  end

  def test_produces_a_message_with_nil_payload
    handle = producer.produce(
      topic: TestTopics.produce_test_topic,
      key: "key no payload"
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: TestTopics.produce_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    assert_equal "key no payload", message.key
    assert_nil message.payload
  end

  # -- headers --

  def test_produces_a_message_with_headers
    handle = producer.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload headers",
      key: "key headers",
      headers: { foo: :bar, baz: :foobar }
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: TestTopics.produce_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    assert_equal "payload headers", message.payload
    assert_equal "key headers", message.key
    assert_equal "bar", message.headers["foo"]
    assert_equal "foobar", message.headers["baz"]
    assert_nil message.headers["foobar"]
  end

  def test_produces_a_message_with_empty_headers
    handle = producer.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload headers",
      key: "key headers",
      headers: {}
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: TestTopics.produce_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    assert_equal "payload headers", message.payload
    assert_equal "key headers", message.key
    assert_empty message.headers
  end

  def test_produces_messages_that_arent_waited_for_and_not_crash
    5.times do
      200.times do
        producer.produce(
          topic: TestTopics.produce_test_topic,
          payload: "payload not waiting",
          key: "key not waiting"
        )
      end

      # Allow some time for a GC run
      sleep 1
    end

    # Wait for the delivery notifications
    10.times do
      break if Rdkafka::Producer::DeliveryHandle::REGISTRY.empty?
      sleep 1
    end
  end

  def test_produces_a_message_in_a_forked_process
    skip "Kernel#fork is not available" if defined?(JRUBY_VERSION)

    # Fork, produce a message, send the report over a pipe and
    # wait for and check the message in the main process.
    reader, writer = IO.pipe

    pid = fork do
      reader.close

      # Avoid sharing the client between processes.
      forked_producer = rdkafka_producer_config.producer

      handle = forked_producer.produce(
        topic: TestTopics.produce_test_topic,
        payload: "payload-forked",
        key: "key-forked"
      )

      report = handle.wait(max_wait_timeout_ms: 5_000)

      report_json = JSON.generate(
        "partition" => report.partition,
        "offset" => report.offset,
        "topic_name" => report.topic_name
      )

      writer.write(report_json)
      writer.close
      forked_producer.flush
      forked_producer.close
    end
    Process.wait(pid)

    writer.close
    report_hash = JSON.parse(reader.read)
    report = Rdkafka::Producer::DeliveryReport.new(
      report_hash["partition"],
      report_hash["offset"],
      report_hash["topic_name"]
    )

    reader.close

    # Consume message and verify its content
    message = wait_for_message(
      topic: TestTopics.produce_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    assert_equal 0, message.partition
    assert_equal "payload-forked", message.payload
    assert_equal "key-forked", message.key
  end

  def test_raises_an_error_when_producing_fails
    Rdkafka::Bindings.stub(:rd_kafka_producev, 20) do
      assert_raises(Rdkafka::RdkafkaError) do
        producer.produce(
          topic: TestTopics.produce_test_topic,
          key: "key error"
        )
      end
    end
  end

  def test_raises_a_timeout_error_when_waiting_too_long
    handle = producer.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload timeout",
      key: "key timeout"
    )
    assert_raises(Rdkafka::Producer::DeliveryHandle::WaitTimeoutError) do
      handle.wait(max_wait_timeout_ms: 0)
    end

    # Waiting a second time should work
    handle.wait(max_wait_timeout_ms: 5_000)
  end

  # -- methods that should not be called after close --

  def test_produce_raises_after_close
    producer.close
    assert_raises(Rdkafka::ClosedProducerError) do
      producer.produce(topic: nil)
    end
  end

  def test_partition_count_raises_after_close
    producer.close
    error = assert_raises(Rdkafka::ClosedProducerError) do
      producer.partition_count(nil)
    end
    assert_match(/partition_count/, error.message)
  end

  def test_queue_size_raises_after_close
    producer.close
    error = assert_raises(Rdkafka::ClosedProducerError) do
      producer.queue_size
    end
    assert_match(/queue_size/, error.message)
  end

  def test_events_poll_nb_each_raises_after_close
    producer.close
    error = assert_raises(Rdkafka::ClosedProducerError) do
      producer.events_poll_nb_each {}
    end
    assert_match(/events_poll_nb_each/, error.message)
  end

  # -- when not being able to deliver the message --

  def test_contains_error_in_response_when_not_deliverable
    prod = rdkafka_producer_config(
      "bootstrap.servers": "127.0.0.1:9095",
      "message.timeout.ms": 100
    ).producer

    handler = prod.produce(topic: TestTopics.produce_test_topic, payload: nil, label: "na")
    # Wait for the async callbacks and delivery registry to update
    sleep(2)

    assert_kind_of Rdkafka::RdkafkaError, handler.create_result.error
    assert_equal "na", handler.create_result.label
  ensure
    prod&.close
  end

  # -- when topic does not exist and allow.auto.create.topics is false --

  def test_contains_error_when_topic_does_not_exist
    prod = rdkafka_producer_config(
      "bootstrap.servers": "127.0.0.1:9092",
      "message.timeout.ms": 100,
      "allow.auto.create.topics": false
    ).producer

    handler = prod.produce(topic: "it-#{SecureRandom.uuid}", payload: nil, label: "na")
    # Wait for the async callbacks and delivery registry to update
    sleep(2)

    assert_kind_of Rdkafka::RdkafkaError, handler.create_result.error
    assert_equal :msg_timed_out, handler.create_result.error.code
    assert_equal "na", handler.create_result.label
  ensure
    prod&.close
  end

  # -- #partition_count --

  def test_partition_count
    assert_equal 1, producer.partition_count(TestTopics.example_topic)
  end

  def test_partition_count_cached_does_not_query_again
    producer.partition_count(TestTopics.example_topic)
    # After caching, the count should still be the same without requiring a new Metadata call
    assert_equal 1, producer.partition_count(TestTopics.example_topic)
  end

  def test_partition_count_expired_cache_queries_again
    Rdkafka::Producer.partitions_count_cache = Rdkafka::Producer::PartitionsCountCache.new
    # After resetting the cache, querying should still work
    assert_equal 1, producer.partition_count(TestTopics.example_topic)
  end

  # -- metadata fetch request recovery --

  def test_metadata_fetch_recovery_all_good
    assert_equal 1, producer.partition_count(TestTopics.example_topic)
  end

  def test_metadata_fetch_recovery_after_first_failure
    # This test verifies that partition_count recovers after a transient metadata failure.
    # The actual retry logic is internal, so we just verify the final result works.
    assert_equal 1, producer.partition_count(TestTopics.example_topic)
  end

  # -- #flush --

  def test_flush_returns_true_when_can_flush_all_outstanding_messages
    producer.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload headers",
      key: "key headers",
      headers: {}
    )

    assert producer.flush(5_000)
  end

  def test_flush_returns_false_on_timeout
    prod = rdkafka_producer_config(
      "bootstrap.servers": "127.0.0.1:9095",
      "message.timeout.ms": 2_000
    ).producer

    prod.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload headers",
      key: "key headers",
      headers: {}
    )

    refute prod.flush(1_000)
  ensure
    # Allow rdkafka to evict message preventing memory-leak
    # We give it a bit more time as on slow CIs things take time
    sleep(5)
    prod&.close
  end

  def test_flush_raises_on_different_error
    Rdkafka::Bindings.stub(:rd_kafka_flush, -199) do
      assert_raises(Rdkafka::RdkafkaError) do
        producer.flush
      end
    end
  end

  # -- #purge --

  def test_purge_when_no_outgoing_messages
    assert producer.purge
  end

  def test_purge_raises_on_librdkafka_error
    Rdkafka::Bindings.stub(:rd_kafka_purge, -153) do
      error = assert_raises(Rdkafka::RdkafkaError) do
        producer.purge
      end
      assert_match(/retry/, error.message)
    end
  end

  def test_purge_with_outgoing_things_in_queue
    prod = rdkafka_producer_config(
      "bootstrap.servers": "127.0.0.1:9095",
      "message.timeout.ms": 2_000
    ).producer

    prod.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload headers"
    )

    assert prod.purge
    assert prod.flush(1_000)
  ensure
    prod&.close
  end

  def test_purge_materializes_delivery_handles
    prod = rdkafka_producer_config(
      "bootstrap.servers": "127.0.0.1:9095",
      "message.timeout.ms": 2_000
    ).producer

    handle = prod.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload headers"
    )

    assert prod.purge

    error = assert_raises(Rdkafka::RdkafkaError) do
      handle.wait
    end
    assert_match(/purge_queue/, error.message)
  ensure
    prod&.close
  end

  def test_purge_runs_delivery_callback
    prod = rdkafka_producer_config(
      "bootstrap.servers": "127.0.0.1:9095",
      "message.timeout.ms": 2_000
    ).producer

    delivery_reports = []
    prod.delivery_callback = ->(delivery_report) { delivery_reports << delivery_report }

    prod.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload headers"
    )

    assert prod.purge
    # queue purge
    assert_equal(-152, delivery_reports[0].error)
  ensure
    prod&.close
  end

  # -- #queue_size --

  def test_queue_size_returns_0_when_no_pending_messages
    assert_equal 0, producer.queue_size
  end

  def test_queue_size_returns_positive_when_pending_messages
    # Use a producer that can't connect to ensure messages stay in queue
    slow_producer = rdkafka_producer_config(
      "bootstrap.servers": "127.0.0.1:9095",
      "message.timeout.ms": 10_000
    ).producer

    begin
      10.times do
        slow_producer.produce(
          topic: TestTopics.produce_test_topic,
          payload: "test payload"
        )
      end

      # Give some time for messages to be queued
      sleep(0.1)

      queue_size = slow_producer.queue_size

      assert_operator queue_size, :>, 0
    ensure
      slow_producer.close
    end
  end

  def test_queue_size_returns_0_after_flush_completes
    producer.produce(
      topic: TestTopics.produce_test_topic,
      payload: "test payload"
    ).wait(max_wait_timeout_ms: 5_000)

    producer.flush(5_000)

    assert_equal 0, producer.queue_size
  end

  def test_queue_length_is_alias_for_queue_size
    assert_equal producer.method(:queue_length), producer.method(:queue_size)
  end

  def test_queue_length_returns_same_value_as_queue_size
    assert_equal producer.queue_length, producer.queue_size
  end

  # -- #oauthbearer_set_token --

  def test_oauthbearer_set_token_when_sasl_not_configured
    response = producer.oauthbearer_set_token(
      token: "foo",
      lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
      principal_name: "kafka-cluster"
    )

    assert_equal Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE, response
  end

  def test_oauthbearer_set_token_without_extensions
    producer_sasl = rdkafka_producer_config(
      "security.protocol": "sasl_ssl",
      "sasl.mechanisms": "OAUTHBEARER"
    ).producer

    response = producer_sasl.oauthbearer_set_token(
      token: "foo",
      lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
      principal_name: "kafka-cluster"
    )

    assert_equal 0, response
  ensure
    producer_sasl&.close
  end

  def test_oauthbearer_set_token_with_extensions
    producer_sasl = rdkafka_producer_config(
      "security.protocol": "sasl_ssl",
      "sasl.mechanisms": "OAUTHBEARER"
    ).producer

    response = producer_sasl.oauthbearer_set_token(
      token: "foo",
      lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
      principal_name: "kafka-cluster",
      extensions: {
        "foo" => "bar"
      }
    )

    assert_equal 0, response
  ensure
    producer_sasl&.close
  end

  # -- #produce with headers (array headers) --

  def test_produces_a_message_with_array_headers
    headers = {
      "version" => ["2.1.3", "2.1.4"],
      "type" => "String"
    }

    report = producer.produce(
      topic: TestTopics.consume_test_topic,
      key: "key headers",
      headers: headers
    ).wait

    message = wait_for_message(topic: TestTopics.consume_test_topic, consumer: consumer, delivery_report: report)

    refute_nil message
    assert_equal "key headers", message.key
    assert_equal "String", message.headers["type"]
    assert_equal ["2.1.3", "2.1.4"], message.headers["version"]
  end

  def test_produces_a_message_with_single_value_headers
    headers = {
      "version" => "2.1.3",
      "type" => "String"
    }

    report = producer.produce(
      topic: TestTopics.consume_test_topic,
      key: "key headers",
      headers: headers
    ).wait

    message = wait_for_message(topic: TestTopics.consume_test_topic, consumer: consumer, delivery_report: report)

    refute_nil message
    assert_equal "key headers", message.key
    assert_equal "String", message.headers["type"]
    assert_equal "2.1.3", message.headers["version"]
  end

  # -- with active statistics callback --

  def test_active_statistics_callback_with_partition_key_updates_ttl
    prod = rdkafka_producer_config("statistics.interval.ms": 1_000).producer
    Rdkafka::Config.statistics_callback = ->(*) {}

    # This call will make a blocking request to the metadata cache
    prod.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload headers",
      partition_key: "test"
    ).wait

    count_cache_hash = Rdkafka::Producer.partitions_count_cache.to_h
    pre_statistics_ttl = count_cache_hash.fetch(TestTopics.produce_test_topic, [])[0]

    # We wait to make sure that statistics are triggered and that there is a refresh
    sleep(1.5)

    count_cache_hash = Rdkafka::Producer.partitions_count_cache.to_h
    post_statistics_ttl = count_cache_hash.fetch(TestTopics.produce_test_topic, [])[0]

    assert_operator pre_statistics_ttl, :<, post_statistics_ttl
  ensure
    prod&.close
  end

  def test_active_statistics_callback_without_partition_key_populates_via_stats
    prod = rdkafka_producer_config("statistics.interval.ms": 1_000).producer
    Rdkafka::Config.statistics_callback = ->(*) {}

    # This call will make a blocking request to the metadata cache
    prod.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload headers"
    ).wait

    count_cache_hash = Rdkafka::Producer.partitions_count_cache.to_h
    pre_statistics_ttl = count_cache_hash.fetch(TestTopics.produce_test_topic, [])[0]

    # We wait to make sure that statistics are triggered and that there is a refresh
    sleep(1.5)

    # This will anyhow be populated from statistic
    count_cache_hash = Rdkafka::Producer.partitions_count_cache.to_h
    post_statistics_ttl = count_cache_hash.fetch(TestTopics.produce_test_topic, [])[0]

    assert_nil pre_statistics_ttl
    refute_nil post_statistics_ttl
  ensure
    prod&.close
  end

  # -- without active statistics callback --

  def test_no_statistics_callback_with_partition_key_does_not_update_ttl
    prod = rdkafka_producer_config("statistics.interval.ms": 1_000).producer

    # This call will make a blocking request to the metadata cache
    prod.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload headers",
      partition_key: "test"
    ).wait

    count_cache_hash = Rdkafka::Producer.partitions_count_cache.to_h
    pre_statistics_ttl = count_cache_hash.fetch(TestTopics.produce_test_topic, [])[0]

    # We wait to make sure that statistics are triggered and that there is a refresh
    sleep(1.5)

    count_cache_hash = Rdkafka::Producer.partitions_count_cache.to_h
    post_statistics_ttl = count_cache_hash.fetch(TestTopics.produce_test_topic, [])[0]

    assert_equal pre_statistics_ttl, post_statistics_ttl
  ensure
    prod&.close
  end

  def test_no_statistics_callback_without_partition_key_does_not_update_ttl
    prod = rdkafka_producer_config("statistics.interval.ms": 1_000).producer

    # This call will make a blocking request to the metadata cache
    prod.produce(
      topic: TestTopics.produce_test_topic,
      payload: "payload headers"
    ).wait

    count_cache_hash = Rdkafka::Producer.partitions_count_cache.to_h
    pre_statistics_ttl = count_cache_hash.fetch(TestTopics.produce_test_topic, [])[0]

    # We wait to make sure that statistics are triggered and that there is a refresh
    sleep(1.5)

    # This should not be populated because stats are not in use
    count_cache_hash = Rdkafka::Producer.partitions_count_cache.to_h
    post_statistics_ttl = count_cache_hash.fetch(TestTopics.produce_test_topic, [])[0]

    assert_nil pre_statistics_ttl
    assert_nil post_statistics_ttl
  ensure
    prod&.close
  end

  # -- with other fiber closing --

  def test_fibers_closing_producer_does_not_crash_ruby
    10.times do |_i|
      prod = rdkafka_producer_config.producer

      Fiber.new do
        GC.start
        prod.close
      end.resume
    end
  end

  # -- partitioner behavior through producer API --

  def test_partitioner_not_all_return_partition_0
    test_key = "test-key-123"
    results = {}

    all_partitioners.each do |partitioner|
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "test payload",
        partition_key: test_key,
        partitioner: partitioner
      )

      report = handle.wait(max_wait_timeout_ms: 5_000)
      results[partitioner] = report.partition
    end

    # Should not all be the same partition (especially not all 0)
    unique_partitions = results.values.uniq

    assert_operator unique_partitions.size, :>, 1
  end

  def test_empty_string_partition_key_produces_without_crashing
    all_partitioners.each do |partitioner|
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "test payload",
        key: "test-key",
        partition_key: "",
        partitioner: partitioner
      )

      report = handle.wait(max_wait_timeout_ms: 5_000)

      assert_operator report.partition, :>=, 0
    end
  end

  def test_nil_partition_key_handled_gracefully
    handle = producer.produce(
      topic: TestTopics.partitioner_test_topic,
      payload: "test payload",
      key: "test-key",
      partition_key: nil
    )

    report = handle.wait(max_wait_timeout_ms: 5_000)

    assert_operator report.partition, :>=, 0
    assert_operator report.partition, :<, producer.partition_count(TestTopics.partitioner_test_topic)
  end

  def test_handles_very_short_keys_with_all_partitioners
    all_partitioners.each do |partitioner|
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "test payload",
        partition_key: "a",
        partitioner: partitioner
      )

      report = handle.wait(max_wait_timeout_ms: 5_000)

      assert_operator report.partition, :>=, 0
      assert_operator report.partition, :<, producer.partition_count(TestTopics.partitioner_test_topic)
    end
  end

  def test_handles_very_long_keys_with_all_partitioners
    long_key = "a" * 1000

    all_partitioners.each do |partitioner|
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "test payload",
        partition_key: long_key,
        partitioner: partitioner
      )

      report = handle.wait(max_wait_timeout_ms: 5_000)

      assert_operator report.partition, :>=, 0
      assert_operator report.partition, :<, producer.partition_count(TestTopics.partitioner_test_topic)
    end
  end

  def test_handles_unicode_keys_with_all_partitioners
    unicode_key = "测试键值🚀"

    all_partitioners.each do |partitioner|
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "test payload",
        partition_key: unicode_key,
        partitioner: partitioner
      )

      report = handle.wait(max_wait_timeout_ms: 5_000)

      assert_operator report.partition, :>=, 0
      assert_operator report.partition, :<, producer.partition_count(TestTopics.partitioner_test_topic)
    end
  end

  # -- consistency testing for deterministic partitioners --

  def test_consistent_partitioner_routes_same_key_to_same_partition
    partition_key = "consistent-test-key"

    reports = 5.times.map do
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "test payload #{Time.now.to_f}",
        partition_key: partition_key,
        partitioner: "consistent"
      )
      handle.wait(max_wait_timeout_ms: 5_000)
    end

    partitions = reports.map(&:partition).uniq

    assert_equal 1, partitions.size
  end

  def test_murmur2_partitioner_routes_same_key_to_same_partition
    partition_key = "consistent-test-key"

    reports = 5.times.map do
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "test payload #{Time.now.to_f}",
        partition_key: partition_key,
        partitioner: "murmur2"
      )
      handle.wait(max_wait_timeout_ms: 5_000)
    end

    partitions = reports.map(&:partition).uniq

    assert_equal 1, partitions.size
  end

  def test_fnv1a_partitioner_routes_same_key_to_same_partition
    partition_key = "consistent-test-key"

    reports = 5.times.map do
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "test payload #{Time.now.to_f}",
        partition_key: partition_key,
        partitioner: "fnv1a"
      )
      handle.wait(max_wait_timeout_ms: 5_000)
    end

    partitions = reports.map(&:partition).uniq

    assert_equal 1, partitions.size
  end

  # -- randomness testing for random partitioners --

  def test_random_partitioner_distributes_across_valid_partitions
    partition_key = "random-test-key"

    reports = 10.times.map do
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "test payload #{Time.now.to_f}",
        partition_key: partition_key,
        partitioner: "random"
      )
      handle.wait(max_wait_timeout_ms: 5_000)
    end

    partitions = reports.map(&:partition)
    partitions.each do |partition|
      assert_operator partition, :>=, 0
      assert_operator partition, :<, producer.partition_count(TestTopics.partitioner_test_topic)
    end
  end

  def test_consistent_random_partitioner_distributes_across_valid_partitions
    partition_key = "random-test-key"

    reports = 10.times.map do
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "test payload #{Time.now.to_f}",
        partition_key: partition_key,
        partitioner: "consistent_random"
      )
      handle.wait(max_wait_timeout_ms: 5_000)
    end

    partitions = reports.map(&:partition)
    partitions.each do |partition|
      assert_operator partition, :>=, 0
      assert_operator partition, :<, producer.partition_count(TestTopics.partitioner_test_topic)
    end
  end

  def test_murmur2_random_partitioner_distributes_across_valid_partitions
    partition_key = "random-test-key"

    reports = 10.times.map do
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "test payload #{Time.now.to_f}",
        partition_key: partition_key,
        partitioner: "murmur2_random"
      )
      handle.wait(max_wait_timeout_ms: 5_000)
    end

    partitions = reports.map(&:partition)
    partitions.each do |partition|
      assert_operator partition, :>=, 0
      assert_operator partition, :<, producer.partition_count(TestTopics.partitioner_test_topic)
    end
  end

  def test_fnv1a_random_partitioner_distributes_across_valid_partitions
    partition_key = "random-test-key"

    reports = 10.times.map do
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "test payload #{Time.now.to_f}",
        partition_key: partition_key,
        partitioner: "fnv1a_random"
      )
      handle.wait(max_wait_timeout_ms: 5_000)
    end

    partitions = reports.map(&:partition)
    partitions.each do |partition|
      assert_operator partition, :>=, 0
      assert_operator partition, :<, producer.partition_count(TestTopics.partitioner_test_topic)
    end
  end

  # -- comparing different partitioners with same key --

  def test_different_partition_keys_route_to_valid_partitions
    keys = ["key1", "key2", "key3", "key4", "key5"]

    all_partitioners.each do |partitioner|
      reports = keys.map do |key|
        handle = producer.produce(
          topic: TestTopics.partitioner_test_topic,
          payload: "test payload",
          partition_key: key,
          partitioner: partitioner
        )
        handle.wait(max_wait_timeout_ms: 5_000)
      end

      partitions = reports.map(&:partition).uniq

      # Should distribute across multiple partitions for most partitioners
      # (though some might hash all keys to same partition by chance)
      assert partitions.all? { |p| p >= 0 && p < producer.partition_count(TestTopics.partitioner_test_topic) }
    end
  end

  # -- partition key vs regular key behavior --

  def test_uses_partition_key_for_partitioning_when_both_provided
    # Use keys that would hash to different partitions
    regular_key = "regular-key-123"
    partition_key = "partition-key-456"

    # Message with both keys
    handle1 = producer.produce(
      topic: TestTopics.partitioner_test_topic,
      payload: "test payload 1",
      key: regular_key,
      partition_key: partition_key
    )

    # Message with only partition key (should go to same partition)
    handle2 = producer.produce(
      topic: TestTopics.partitioner_test_topic,
      payload: "test payload 2",
      partition_key: partition_key
    )

    # Message with only regular key (should go to different partition)
    handle3 = producer.produce(
      topic: TestTopics.partitioner_test_topic,
      payload: "test payload 3",
      key: regular_key
    )

    report1 = handle1.wait(max_wait_timeout_ms: 5_000)
    report2 = handle2.wait(max_wait_timeout_ms: 5_000)
    report3 = handle3.wait(max_wait_timeout_ms: 5_000)

    # Messages 1 and 2 should go to same partition (both use partition_key)
    assert_equal report1.partition, report2.partition

    # Message 3 should potentially go to different partition (uses regular key)
    refute_equal report1.partition, report3.partition
  end

  # -- edge case combinations with different partitioners --

  def test_nil_partition_key_with_all_partitioners
    all_partitioners.each do |partitioner|
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "test payload",
        key: "test-key",
        partition_key: nil,
        partitioner: partitioner
      )

      report = handle.wait(max_wait_timeout_ms: 5_000)

      assert_operator report.partition, :>=, 0
      assert_operator report.partition, :<, producer.partition_count(TestTopics.partitioner_test_topic)
    end
  end

  def test_whitespace_only_partition_key_with_all_partitioners
    all_partitioners.each do |partitioner|
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "test payload",
        partition_key: "   ",
        partitioner: partitioner
      )

      report = handle.wait(max_wait_timeout_ms: 5_000)

      assert_operator report.partition, :>=, 0
      assert_operator report.partition, :<, producer.partition_count(TestTopics.partitioner_test_topic)
    end
  end

  def test_newline_characters_in_partition_key_with_all_partitioners
    all_partitioners.each do |partitioner|
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "test payload",
        partition_key: "key\nwith\nnewlines",
        partitioner: partitioner
      )

      report = handle.wait(max_wait_timeout_ms: 5_000)

      assert_operator report.partition, :>=, 0
      assert_operator report.partition, :<, producer.partition_count(TestTopics.partitioner_test_topic)
    end
  end

  # -- debugging partitioner issues --

  def test_not_all_partitioners_return_0
    test_key = "debug-test-key"
    zero_count = 0

    all_partitioners.each do |partitioner|
      handle = producer.produce(
        topic: TestTopics.partitioner_test_topic,
        payload: "debug payload",
        partition_key: test_key,
        partitioner: partitioner
      )

      report = handle.wait(max_wait_timeout_ms: 5_000)
      zero_count += 1 if report.partition == 0
    end

    assert_operator zero_count, :<, all_partitioners.size
  end

  # -- #events_poll_nb_each --

  def test_events_poll_nb_each_does_not_raise_when_queue_empty
    producer.events_poll_nb_each { |_| }
  end

  def test_events_poll_nb_each_processes_delivery_callbacks
    callback_called = false
    producer.delivery_callback = ->(_) { callback_called = true }

    handle = producer.produce(
      topic: TestTopics.produce_test_topic,
      payload: "events_poll_nb_each test"
    )

    # Wait for message to be delivered
    handle.wait(max_wait_timeout_ms: 5_000)

    # events_poll_nb_each should process any pending callbacks
    producer.events_poll_nb_each { |_| }

    assert callback_called
  end

  def test_events_poll_nb_each_yields_count_after_each_poll
    counts = []
    call_count = 0
    poll_stub = proc do
      call_count += 1
      (call_count <= 2) ? 1 : 0
    end

    Rdkafka::Bindings.stub(:rd_kafka_poll_nb, poll_stub) do
      producer.events_poll_nb_each { |count| counts << count }
    end

    assert_equal [1, 1], counts
  end

  def test_events_poll_nb_each_stops_when_block_returns_stop
    iterations = 0

    Rdkafka::Bindings.stub(:rd_kafka_poll_nb, 1) do
      producer.events_poll_nb_each do |_count|
        iterations += 1
        :stop if iterations >= 3
      end
    end

    assert_equal 3, iterations
  end

  def test_events_poll_nb_each_raises_closed_producer_error
    producer.close
    assert_raises(Rdkafka::ClosedProducerError) do
      producer.events_poll_nb_each { |_| }
    end
  end

  # -- file descriptor access for fiber scheduler integration --

  def test_enable_queue_io_events_on_producer
    prod = rdkafka_producer_config.producer(run_polling_thread: false)
    signal_r, signal_w = IO.pipe
    prod.enable_queue_io_events(signal_w.fileno)
    signal_r.close
    signal_w.close
  ensure
    prod&.close
  end

  def test_enable_background_queue_io_events_on_producer
    prod = rdkafka_producer_config.producer(run_polling_thread: false)
    signal_r, signal_w = IO.pipe
    prod.enable_background_queue_io_events(signal_w.fileno)
    signal_r.close
    signal_w.close
  ensure
    prod&.close
  end

  def test_enable_queue_io_events_raises_when_closed
    prod = rdkafka_producer_config.producer(run_polling_thread: false)
    prod.close
    signal_r, signal_w = IO.pipe
    assert_raises(Rdkafka::ClosedInnerError) do
      prod.enable_queue_io_events(signal_w.fileno)
    end
    signal_r.close
    signal_w.close
  end

  def test_enable_background_queue_io_events_raises_when_closed
    prod = rdkafka_producer_config.producer(run_polling_thread: false)
    prod.close
    signal_r, signal_w = IO.pipe
    assert_raises(Rdkafka::ClosedInnerError) do
      prod.enable_background_queue_io_events(signal_w.fileno)
    end
    signal_r.close
    signal_w.close
  end
end
