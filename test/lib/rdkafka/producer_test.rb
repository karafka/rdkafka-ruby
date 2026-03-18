# frozen_string_literal: true

require "zlib"

require_relative "../../test_helper"

describe Rdkafka::Producer do
  def producer
    @producer ||= rdkafka_producer_config.producer
  end

  def consumer
    @consumer ||= rdkafka_consumer_config.consumer
  end

  def topic
    @topic ||= TestTopics.create
  end

  def topic_25
    @topic_25 ||= TestTopics.create(partitions: 25)
  end

  before do
    @all_partitioners = %w[random consistent consistent_random murmur2 murmur2_random fnv1a fnv1a_random]
  end

  after do
    # Registry should always end up being empty.
    # Async delivery callbacks may not have fired yet, so poll briefly.
    registry = Rdkafka::Producer::DeliveryHandle::REGISTRY
    10.times do
      break if registry.empty?

      sleep(0.05)
    end
    assert_empty registry, registry.inspect
    producer.close
    consumer.close
  end

  describe "producer without auto-start" do
    it "expect to be able to start it later and close" do
      @producer = rdkafka_producer_config.producer(native_kafka_auto_start: false)
      producer.start
      producer.close
    end

    it "expect to be able to close it without starting" do
      @producer = rdkafka_producer_config.producer(native_kafka_auto_start: false)
      producer.close
    end
  end

  describe "#name" do
    it "includes rdkafka#producer-" do
      assert_includes producer.name, "rdkafka#producer-"
    end
  end

  describe "#produce with topic config alterations" do
    context "when config is not valid" do
      it "expect to raise error" do
        assert_raises(Rdkafka::Config::ConfigError) do
          producer.produce(topic: "test", payload: "", topic_config: { invalid: "invalid" })
        end
      end
    end

    context "when config is valid" do
      it "expect not to raise error" do
        producer.produce(topic: "test", payload: "", topic_config: { acks: 1 }).wait
      end

      context "when alteration should change behavior" do
        it "expect to give up on delivery fast based on alteration config" do
          @producer = rdkafka_producer_config(
            "message.timeout.ms": 1_000_000,
            "bootstrap.servers": "127.0.0.1:9094"
          ).producer

          e = assert_raises(Rdkafka::RdkafkaError) do
            producer.produce(
              topic: "produce_config_test",
              payload: "test",
              topic_config: {
                "compression.type": "gzip",
                "message.timeout.ms": 1
              }
            ).wait
          end
          assert_match(/msg_timed_out/, e.message)
        end
      end
    end
  end

  context "delivery callback" do
    context "with a proc/lambda" do
      it "sets the callback" do
        producer.delivery_callback = lambda do |delivery_handle|
        end
        assert_respond_to producer.delivery_callback, :call
      end

      it "calls the callback when a message is delivered" do
        @callback_called = false

        producer.delivery_callback = lambda do |report|
          refute_nil report
          assert_equal "label", report.label
          assert_equal 1, report.partition
          assert_operator report.offset, :>=, 0
          assert_equal topic, report.topic_name
          @callback_called = true
        end

        # Produce a message
        handle = producer.produce(
          topic: topic,
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
        assert_equal true, @callback_called
      end

      it "provides handle" do
        @callback_handle = nil

        producer.delivery_callback = lambda { |_, handle| @callback_handle = handle }

        # Produce a message
        handle = producer.produce(
          topic: topic,
          payload: "payload",
          key: "key"
        )

        # Wait for it to be delivered
        handle.wait(max_wait_timeout_ms: 15_000)

        # Join the producer thread.
        producer.close

        assert_same handle, @callback_handle
      end
    end

    context "with a callable object" do
      it "sets the callback" do
        callback = Class.new do
          def call(stats)
          end
        end
        producer.delivery_callback = callback.new
        assert_respond_to producer.delivery_callback, :call
      end

      it "calls the callback when a message is delivered" do
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
          topic: topic,
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
        assert_equal topic, called_report.first.topic_name
      end

      it "provides handle" do
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
          topic: topic,
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
    end

    it "does not accept a callback that's not callable" do
      assert_raises(TypeError) do
        producer.delivery_callback = "a string"
      end
    end
  end

  it "requires a topic" do
    e = assert_raises(ArgumentError) do
      producer.produce(
        payload: "payload",
        key: "key"
      )
    end
    assert_match(/missing keyword: :?topic/, e.message)
  end

  it "produces a message" do
    # Produce a message
    handle = producer.produce(
      topic: topic,
      payload: "payload",
      key: "key",
      label: "label"
    )

    # Should be pending at first
    assert_equal true, handle.pending?
    assert_equal "label", handle.label

    # Check delivery handle and report
    report = handle.wait(max_wait_timeout_ms: 5_000)
    assert_equal false, handle.pending?
    refute_nil report
    assert_equal 1, report.partition
    assert_operator report.offset, :>=, 0
    assert_equal "label", report.label

    # Flush and close producer
    producer.flush
    producer.close

    # Consume message and verify its content
    message = wait_for_message(
      topic: topic,
      delivery_report: report,
      consumer: consumer
    )
    assert_equal 1, message.partition
    assert_equal "payload", message.payload
    assert_equal "key", message.key
    assert_in_delta Time.now, message.timestamp, 10
  end

  it "produces a message with a specified partition" do
    # Produce a message
    handle = producer.produce(
      topic: topic,
      payload: "payload partition",
      key: "key partition",
      partition: 1
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: topic,
      delivery_report: report,
      consumer: consumer
    )
    assert_equal 1, message.partition
    assert_equal "key partition", message.key
  end

  it "produces a message to the same partition with a similar partition key" do
    # Avoid partitioner collisions.
    while true
      key = ("a".."z").to_a.shuffle.take(10).join("")
      partition_key = ("a".."z").to_a.shuffle.take(10).join("")
      partition_count = producer.partition_count(topic_25)
      break if (Zlib.crc32(key) % partition_count) != (Zlib.crc32(partition_key) % partition_count)
    end

    # Produce a message with key, partition_key and key + partition_key
    messages = [{ key: key }, { partition_key: partition_key }, { key: key, partition_key: partition_key }]

    messages = messages.map do |m|
      handle = producer.produce(
        topic: topic_25,
        payload: "payload partition",
        key: m[:key],
        partition_key: m[:partition_key]
      )
      report = handle.wait(max_wait_timeout_ms: 5_000)

      wait_for_message(
        topic: topic_25,
        delivery_report: report
      )
    end

    refute_equal messages[0].partition, messages[2].partition
    assert_equal messages[1].partition, messages[2].partition
    assert_equal key, messages[0].key
    assert_nil messages[1].key
    assert_equal key, messages[2].key
  end

  it "produces a message with empty string without crashing" do
    messages = [{ key: "a", partition_key: "" }]

    messages = messages.map do |m|
      handle = producer.produce(
        topic: topic_25,
        payload: "payload partition",
        key: m[:key],
        partition_key: m[:partition_key]
      )
      report = handle.wait(max_wait_timeout_ms: 5_000)

      wait_for_message(
        topic: topic_25,
        delivery_report: report
      )
    end

    assert_operator messages[0].partition, :>=, 0
    assert_equal "a", messages[0].key
  end

  it "produces a message with utf-8 encoding" do
    handle = producer.produce(
      topic: topic,
      payload: "Τη γλώσσα μου έδωσαν ελληνική",
      key: "key utf8"
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: topic,
      delivery_report: report,
      consumer: consumer
    )

    assert_equal 1, message.partition
    assert_equal "Τη γλώσσα μου έδωσαν ελληνική", message.payload.force_encoding("utf-8")
    assert_equal "key utf8", message.key
  end

  it "produces a message to a non-existing topic with key and partition key" do
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
    assert_equal true, handle.pending?
    assert_equal "label", handle.label

    # Check delivery handle and report
    report = handle.wait(max_wait_timeout_ms: 5_000)
    assert_equal false, handle.pending?
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

  context "timestamp" do
    it "raises a type error if not nil, integer or time" do
      assert_raises(TypeError) do
        producer.produce(
          topic: topic,
          payload: "payload timestamp",
          key: "key timestamp",
          timestamp: "10101010"
        )
      end
    end

    it "produces a message with an integer timestamp" do
      handle = producer.produce(
        topic: topic,
        payload: "payload timestamp",
        key: "key timestamp",
        timestamp: 1505069646252
      )
      report = handle.wait(max_wait_timeout_ms: 5_000)

      # Consume message and verify its content
      message = wait_for_message(
        topic: topic,
        delivery_report: report
      )

      assert_equal 2, message.partition
      assert_equal "key timestamp", message.key
      assert_equal Time.at(1505069646, 252_000), message.timestamp
    end

    it "produces a message with a time timestamp" do
      handle = producer.produce(
        topic: topic,
        payload: "payload timestamp",
        key: "key timestamp",
        timestamp: Time.at(1505069646, 353_000)
      )
      report = handle.wait(max_wait_timeout_ms: 5_000)

      # Consume message and verify its content
      message = wait_for_message(
        topic: topic,
        delivery_report: report
      )

      assert_equal 2, message.partition
      assert_equal "key timestamp", message.key
      assert_equal Time.at(1505069646, 353_000), message.timestamp
    end
  end

  it "produces a message with nil key" do
    handle = producer.produce(
      topic: topic,
      payload: "payload no key"
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: topic,
      delivery_report: report,
      consumer: consumer
    )

    assert_nil message.key
    assert_equal "payload no key", message.payload
  end

  it "produces a message with nil payload" do
    handle = producer.produce(
      topic: topic,
      key: "key no payload"
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: topic,
      delivery_report: report,
      consumer: consumer
    )

    assert_equal "key no payload", message.key
    assert_nil message.payload
  end

  it "produces a message with headers" do
    handle = producer.produce(
      topic: topic,
      payload: "payload headers",
      key: "key headers",
      headers: { foo: :bar, baz: :foobar }
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: topic,
      delivery_report: report,
      consumer: consumer
    )

    assert_equal "payload headers", message.payload
    assert_equal "key headers", message.key
    assert_equal "bar", message.headers["foo"]
    assert_equal "foobar", message.headers["baz"]
    assert_nil message.headers["foobar"]
  end

  it "produces a message with empty headers" do
    handle = producer.produce(
      topic: topic,
      payload: "payload headers",
      key: "key headers",
      headers: {}
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: topic,
      delivery_report: report,
      consumer: consumer
    )

    assert_equal "payload headers", message.payload
    assert_equal "key headers", message.key
    assert_empty message.headers
  end

  it "produces message that aren't waited for and not crash" do
    5.times do
      200.times do
        producer.produce(
          topic: topic,
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

  it "produces a message in a forked process" do
    skip "Kernel#fork is not available" if defined?(JRUBY_VERSION)
    # Fork, produce a message, send the report over a pipe and
    # wait for and check the message in the main process.
    # Force topic creation before forking
    fork_topic = topic
    reader, writer = IO.pipe

    pid = fork do
      reader.close

      # Avoid sharing the client between processes.
      fork_producer = rdkafka_producer_config.producer

      handle = fork_producer.produce(
        topic: fork_topic,
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
      fork_producer.flush
      fork_producer.close
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
      topic: topic,
      delivery_report: report,
      consumer: consumer
    )
    assert_equal 0, message.partition
    assert_equal "payload-forked", message.payload
    assert_equal "key-forked", message.key
  end

  it "raises an error when producing fails" do
    Rdkafka::Bindings.expects(:rd_kafka_producev).returns(20)

    assert_raises(Rdkafka::RdkafkaError) do
      producer.produce(
        topic: topic,
        key: "key error"
      )
    end
  end

  it "raises a timeout error when waiting too long" do
    handle = producer.produce(
      topic: topic,
      payload: "payload timeout",
      key: "key timeout"
    )

    # With a warmed-up broker connection the message may already be delivered
    # before we get to call wait, so only assert timeout if still pending
    if handle[:pending]
      assert_raises(Rdkafka::Producer::DeliveryHandle::WaitTimeoutError) do
        handle.wait(max_wait_timeout_ms: 0)
      end
    end

    # Waiting with a real timeout should always work
    handle.wait(max_wait_timeout_ms: 5_000)
  end

  context "methods that should not be called after a producer has been closed" do
    before do
      producer.close
    end

    # Affected methods and a non-invalid set of parameters for the method
    # :no_args indicates the method takes no arguments
    {
      produce: { topic: nil },
      partition_count: nil,
      queue_size: :no_args,
      events_poll_nb_each: :no_args
    }.each do |method, args|
      it "raises an exception if #{method} is called" do
        e = assert_raises(Rdkafka::ClosedProducerError) do
          if args == :no_args
            producer.public_send(method)
          elsif args.is_a?(Hash)
            producer.public_send(method, **args)
          else
            producer.public_send(method, args)
          end
        end
        assert_match(/#{method}/, e.message)
      end
    end
  end

  context "when not being able to deliver the message" do
    it "contains the error in the response when not deliverable" do
      @producer = rdkafka_producer_config(
        "bootstrap.servers": "127.0.0.1:9095",
        "message.timeout.ms": 100
      ).producer

      handler = producer.produce(topic: topic, payload: nil, label: "na")
      # Wait for the async callbacks and delivery registry to update
      sleep(2)
      assert_kind_of Rdkafka::RdkafkaError, handler.create_result.error
      assert_equal "na", handler.create_result.label
    end
  end

  context "when topic does not exist and allow.auto.create.topics is false" do
    it "contains the error in the response when not deliverable" do
      @producer = rdkafka_producer_config(
        "bootstrap.servers": "127.0.0.1:9092",
        "message.timeout.ms": 100,
        "allow.auto.create.topics": false
      ).producer

      handler = producer.produce(topic: "it-#{SecureRandom.uuid}", payload: nil, label: "na")
      # Wait for the async callbacks and delivery registry to update
      sleep(2)
      assert_kind_of Rdkafka::RdkafkaError, handler.create_result.error
      assert_equal :msg_timed_out, handler.create_result.error.code
      assert_equal "na", handler.create_result.label
    end
  end

  describe "#partition_count" do
    it "returns partition count" do
      assert_equal 1, producer.partition_count(TestTopics.example_topic)
    end

    context "when the partition count value is already cached" do
      before do
        producer.partition_count(TestTopics.example_topic)
      end

      it "expect not to query it again" do
        ::Rdkafka::Metadata.expects(:new).never
        producer.partition_count(TestTopics.example_topic)
      end
    end

    context "when the partition count value was cached but time expired" do
      before do
        ::Rdkafka::Producer.partitions_count_cache = Rdkafka::Producer::PartitionsCountCache.new
      end

      it "expect to query it again" do
        ::Rdkafka::Metadata.expects(:new).returns(mock("metadata", topics: [{ partition_count: 1 }]))
        producer.partition_count(TestTopics.example_topic)
      end
    end

    context "when the partition count value was cached and time did not expire" do
      before do
        ::Process.stubs(:clock_gettime).returns(0, 29.001)
        producer.partition_count(TestTopics.example_topic)
      end

      it "expect not to query it again" do
        ::Rdkafka::Metadata.expects(:new).never
        producer.partition_count(TestTopics.example_topic)
      end
    end
  end

  describe "metadata fetch request recovery" do
    describe "metadata initialization recovery" do
      context "when all good" do
        it "returns partition count" do
          assert_equal 1, producer.partition_count(TestTopics.example_topic)
        end
      end

      context "when we fail for the first time with handled error" do
        it "returns partition count" do
          original = Rdkafka::Bindings.method(:rd_kafka_metadata)
          call_count = 0
          meta = Rdkafka::Bindings.singleton_class

          verbose_was, $VERBOSE = $VERBOSE, nil
          meta.send(:remove_method, :rd_kafka_metadata)
          meta.send(:define_method, :rd_kafka_metadata) do |*args|
            call_count += 1
            if call_count == 1
              -185
            else
              original.call(*args)
            end
          end
          $VERBOSE = verbose_was

          begin
            assert_equal 1, producer.partition_count(TestTopics.example_topic)
          ensure
            verbose_was, $VERBOSE = $VERBOSE, nil
            meta.send(:remove_method, :rd_kafka_metadata)
            meta.send(:define_method, :rd_kafka_metadata, original)
            $VERBOSE = verbose_was
          end
        end
      end
    end
  end

  describe "#flush" do
    it "returns flush when it can flush all outstanding messages or when no messages" do
      producer.produce(
        topic: topic,
        payload: "payload headers",
        key: "key headers",
        headers: {}
      )

      assert_equal true, producer.flush(5_000)
    end

    context "when it cannot flush due to a timeout" do
      after do
        # Allow rdkafka to evict message preventing memory-leak
        # We give it a bit more time as on slow CIs things take time
        sleep(5)
      end

      it "returns false on flush when cannot deliver and beyond timeout" do
        @producer = rdkafka_producer_config(
          "bootstrap.servers": "127.0.0.1:9095",
          "message.timeout.ms": 2_000
        ).producer

        producer.produce(
          topic: topic,
          payload: "payload headers",
          key: "key headers",
          headers: {}
        )

        assert_equal false, producer.flush(1_000)
      end
    end

    context "when there is a different error" do
      before { Rdkafka::Bindings.stubs(:rd_kafka_flush).returns(-199) }

      it "raises it" do
        assert_raises(Rdkafka::RdkafkaError) do
          producer.flush
        end
      end
    end
  end

  describe "#purge" do
    context "when no outgoing messages" do
      it "returns true" do
        assert_equal true, producer.purge
      end
    end

    context "when librdkafka purge returns an error" do
      before { Rdkafka::Bindings.expects(:rd_kafka_purge).returns(-153) }

      it "expect to raise an error" do
        e = assert_raises(Rdkafka::RdkafkaError) do
          producer.purge
        end
        assert_match(/retry/, e.message)
      end
    end

    context "when there are outgoing things in the queue" do
      it "shoulds purge and move forward" do
        @producer = rdkafka_producer_config(
          "bootstrap.servers": "127.0.0.1:9095",
          "message.timeout.ms": 2_000
        ).producer

        producer.produce(
          topic: topic,
          payload: "payload headers"
        )

        assert_equal true, producer.purge
        assert_equal true, producer.flush(1_000)
      end

      it "materializes the delivery handles" do
        @producer = rdkafka_producer_config(
          "bootstrap.servers": "127.0.0.1:9095",
          "message.timeout.ms": 2_000
        ).producer

        handle = producer.produce(
          topic: topic,
          payload: "payload headers"
        )

        assert_equal true, producer.purge

        e = assert_raises(Rdkafka::RdkafkaError) do
          handle.wait
        end
        assert_match(/purge_queue/, e.message)
      end

      context "when using delivery_callback" do
        it "runs the callback" do
          @producer = rdkafka_producer_config(
            "bootstrap.servers": "127.0.0.1:9095",
            "message.timeout.ms": 2_000
          ).producer

          delivery_reports = []
          delivery_callback = ->(delivery_report) { delivery_reports << delivery_report }

          producer.delivery_callback = delivery_callback

          producer.produce(
            topic: topic,
            payload: "payload headers"
          )

          assert_equal true, producer.purge
          # queue purge
          assert_equal(-152, delivery_reports[0].error)
        end
      end
    end
  end

  describe "#queue_size" do
    it "returns 0 when there are no pending messages" do
      assert_equal 0, producer.queue_size
    end

    it "returns a positive number when there are pending messages" do
      # Use a producer that can't connect to ensure messages stay in queue
      slow_producer = rdkafka_producer_config(
        "bootstrap.servers": "127.0.0.1:9095",
        "message.timeout.ms": 10_000
      ).producer

      begin
        10.times do
          slow_producer.produce(
            topic: topic,
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

    it "returns 0 after flush completes" do
      producer.produce(
        topic: topic,
        payload: "test payload"
      ).wait(max_wait_timeout_ms: 5_000)

      producer.flush(5_000)

      assert_equal 0, producer.queue_size
    end

    describe "#queue_length alias" do
      it "is an alias for queue_size" do
        assert_equal producer.method(:queue_length), producer.method(:queue_size)
      end

      it "returns the same value as queue_size" do
        assert_equal producer.queue_length, producer.queue_size
      end
    end
  end

  describe "#oauthbearer_set_token" do
    context "when sasl not configured" do
      it "returns RD_KAFKA_RESP_ERR__STATE" do
        response = producer.oauthbearer_set_token(
          token: "foo",
          lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
          principal_name: "kafka-cluster"
        )
        assert_equal Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE, response
      end
    end

    context "when sasl configured" do
      before do
        @producer_sasl = rdkafka_producer_config(
          "security.protocol": "sasl_ssl",
          "sasl.mechanisms": "OAUTHBEARER"
        ).producer
      end

      after do
        @producer_sasl.close
      end

      context "without extensions" do
        it "succeeds" do
          response = @producer_sasl.oauthbearer_set_token(
            token: "foo",
            lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
            principal_name: "kafka-cluster"
          )
          assert_equal 0, response
        end
      end

      context "with extensions" do
        it "succeeds" do
          response = @producer_sasl.oauthbearer_set_token(
            token: "foo",
            lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
            principal_name: "kafka-cluster",
            extensions: {
              "foo" => "bar"
            }
          )
          assert_equal 0, response
        end
      end
    end
  end

  describe "#produce with headers" do
    it "produces a message with array headers" do
      headers = {
        "version" => ["2.1.3", "2.1.4"],
        "type" => "String"
      }

      report = producer.produce(
        topic: topic,
        key: "key headers",
        headers: headers
      ).wait

      message = wait_for_message(topic: topic, consumer: consumer, delivery_report: report)
      refute_nil message
      assert_equal "key headers", message.key
      assert_equal "String", message.headers["type"]
      assert_equal ["2.1.3", "2.1.4"], message.headers["version"]
    end

    it "produces a message with single value headers" do
      headers = {
        "version" => "2.1.3",
        "type" => "String"
      }

      report = producer.produce(
        topic: topic,
        key: "key headers",
        headers: headers
      ).wait

      message = wait_for_message(topic: topic, consumer: consumer, delivery_report: report)
      refute_nil message
      assert_equal "key headers", message.key
      assert_equal "String", message.headers["type"]
      assert_equal "2.1.3", message.headers["version"]
    end
  end

  describe "with active statistics callback" do
    it "expect to update ttl on the partitions count cache via statistics" do
      @producer = rdkafka_producer_config("statistics.interval.ms": 1_000).producer
      Rdkafka::Config.statistics_callback = ->(*) {}

      # This call will make a blocking request to the metadata cache
      producer.produce(
        topic: topic,
        payload: "payload headers",
        partition_key: "test"
      ).wait

      count_cache_hash = described_class.partitions_count_cache.to_h
      pre_statistics_ttl = count_cache_hash.fetch(topic, [])[0]

      # We wait to make sure that statistics are triggered and that there is a refresh
      sleep(1.5)

      count_cache_hash = described_class.partitions_count_cache.to_h
      post_statistics_ttl = count_cache_hash.fetch(topic, [])[0]

      assert_operator pre_statistics_ttl, :<, post_statistics_ttl
    end

    it "expect not to update ttl on the partitions count cache via blocking but via use stats" do
      @producer = rdkafka_producer_config("statistics.interval.ms": 1_000).producer
      Rdkafka::Config.statistics_callback = ->(*) {}

      # This call will make a blocking request to the metadata cache
      producer.produce(
        topic: topic,
        payload: "payload headers"
      ).wait

      count_cache_hash = described_class.partitions_count_cache.to_h
      pre_statistics_ttl = count_cache_hash.fetch(topic, [])[0]

      # We wait to make sure that statistics are triggered and that there is a refresh
      sleep(1.5)

      # This will anyhow be populated from statistic
      count_cache_hash = described_class.partitions_count_cache.to_h
      post_statistics_ttl = count_cache_hash.fetch(topic, [])[0]

      assert_nil pre_statistics_ttl
      refute_nil post_statistics_ttl
    end
  end

  describe "without active statistics callback" do
    it "expect not to update ttl on the partitions count cache via statistics" do
      @producer = rdkafka_producer_config("statistics.interval.ms": 1_000).producer

      # This call will make a blocking request to the metadata cache
      producer.produce(
        topic: topic,
        payload: "payload headers",
        partition_key: "test"
      ).wait

      count_cache_hash = described_class.partitions_count_cache.to_h
      pre_statistics_ttl = count_cache_hash.fetch(topic, [])[0]

      # We wait to make sure that statistics are triggered and that there is a refresh
      sleep(1.5)

      count_cache_hash = described_class.partitions_count_cache.to_h
      post_statistics_ttl = count_cache_hash.fetch(topic, [])[0]

      assert_equal pre_statistics_ttl, post_statistics_ttl
    end

    it "expect not to update ttl on the partitions count cache via anything" do
      @producer = rdkafka_producer_config("statistics.interval.ms": 1_000).producer

      # This call will make a blocking request to the metadata cache
      producer.produce(
        topic: topic,
        payload: "payload headers"
      ).wait

      count_cache_hash = described_class.partitions_count_cache.to_h
      pre_statistics_ttl = count_cache_hash.fetch(topic, [])[0]

      # We wait to make sure that statistics are triggered and that there is a refresh
      sleep(1.5)

      # This should not be populated because stats are not in use
      count_cache_hash = described_class.partitions_count_cache.to_h
      post_statistics_ttl = count_cache_hash.fetch(topic, [])[0]

      assert_nil pre_statistics_ttl
      assert_nil post_statistics_ttl
    end
  end

  describe "with other fiber closing" do
    context "when we create many fibers and close producer in some of them" do
      it "expect not to crash ruby" do
        10.times do |i|
          fiber_producer = rdkafka_producer_config.producer

          Fiber.new do
            GC.start
            fiber_producer.close
          end.resume
        end
      end
    end
  end

  describe "partitioner behavior through producer API" do
    context "testing all partitioners with same key" do
      it "does not return partition 0 for all partitioners" do
        test_key = "test-key-123"
        results = {}

        @all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: topic_25,
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
    end

    context "empty string partition key" do
      it "produces message with empty partition key without crashing and go to partition 0 for all partitioners" do
        @all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: topic_25,
            payload: "test payload",
            key: "test-key",
            partition_key: "",
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout_ms: 5_000)
          assert_operator report.partition, :>=, 0
        end
      end
    end

    context "nil partition key" do
      it "handles nil partition key gracefully" do
        handle = producer.produce(
          topic: topic_25,
          payload: "test payload",
          key: "test-key",
          partition_key: nil
        )

        report = handle.wait(max_wait_timeout_ms: 5_000)
        assert_operator report.partition, :>=, 0
        assert_operator report.partition, :<, producer.partition_count(topic_25)
      end
    end

    context "various key types and lengths with different partitioners" do
      it "handles very short keys with all partitioners" do
        @all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: topic_25,
            payload: "test payload",
            partition_key: "a",
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout_ms: 5_000)
          assert_operator report.partition, :>=, 0
          assert_operator report.partition, :<, producer.partition_count(topic_25)
        end
      end

      it "handles very long keys with all partitioners" do
        long_key = "a" * 1000

        @all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: topic_25,
            payload: "test payload",
            partition_key: long_key,
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout_ms: 5_000)
          assert_operator report.partition, :>=, 0
          assert_operator report.partition, :<, producer.partition_count(topic_25)
        end
      end

      it "handles unicode keys with all partitioners" do
        unicode_key = "测试键值🚀"

        @all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: topic_25,
            payload: "test payload",
            partition_key: unicode_key,
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout_ms: 5_000)
          assert_operator report.partition, :>=, 0
          assert_operator report.partition, :<, producer.partition_count(topic_25)
        end
      end
    end

    context "consistency testing for deterministic partitioners" do
      %w[consistent murmur2 fnv1a].each do |partitioner|
        it "consistentlies route same partition key to same partition with #{partitioner}" do
          partition_key = "consistent-test-key"

          # Produce multiple messages with same partition key
          reports = 5.times.map do
            handle = producer.produce(
              topic: topic_25,
              payload: "test payload #{Time.now.to_f}",
              partition_key: partition_key,
              partitioner: partitioner
            )
            handle.wait(max_wait_timeout_ms: 5_000)
          end

          # All should go to same partition
          partitions = reports.map(&:partition).uniq
          assert_equal 1, partitions.size
        end
      end
    end

    context "randomness testing for random partitioners" do
      %w[random consistent_random murmur2_random fnv1a_random].each do |partitioner|
        it "potentiallies distribute across partitions with #{partitioner}" do
          # Note: random partitioners might still return same value by chance
          partition_key = "random-test-key"

          reports = 10.times.map do
            handle = producer.produce(
              topic: topic_25,
              payload: "test payload #{Time.now.to_f}",
              partition_key: partition_key,
              partitioner: partitioner
            )
            handle.wait(max_wait_timeout_ms: 5_000)
          end

          partitions = reports.map(&:partition)

          # Just ensure they're valid partitions
          partitions.each do |partition|
            assert_operator partition, :>=, 0
            assert_operator partition, :<, producer.partition_count(topic_25)
          end
        end
      end
    end

    context "comparing different partitioners with same key" do
      it "routes different partition keys to potentially different partitions" do
        keys = ["key1", "key2", "key3", "key4", "key5"]

        @all_partitioners.each do |partitioner|
          reports = keys.map do |key|
            handle = producer.produce(
              topic: topic_25,
              payload: "test payload",
              partition_key: key,
              partitioner: partitioner
            )
            handle.wait(max_wait_timeout_ms: 5_000)
          end

          partitions = reports.map(&:partition).uniq

          # Should distribute across multiple partitions for most partitioners
          # (though some might hash all keys to same partition by chance)
          assert_equal true, partitions.all? { |p| p >= 0 && p < producer.partition_count(topic_25) }
        end
      end
    end

    context "partition key vs regular key behavior" do
      it "uses partition key for partitioning when both key and partition_key are provided" do
        # Use keys that would hash to different partitions
        regular_key = "regular-key-123"
        partition_key = "partition-key-456"

        # Message with both keys
        handle1 = producer.produce(
          topic: topic_25,
          payload: "test payload 1",
          key: regular_key,
          partition_key: partition_key
        )

        # Message with only partition key (should go to same partition)
        handle2 = producer.produce(
          topic: topic_25,
          payload: "test payload 2",
          partition_key: partition_key
        )

        # Message with only regular key (should go to different partition)
        handle3 = producer.produce(
          topic: topic_25,
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
    end

    context "edge case combinations with different partitioners" do
      it "handles nil partition key with all partitioners" do
        @all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: topic_25,
            payload: "test payload",
            key: "test-key",
            partition_key: nil,
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout_ms: 5_000)
          assert_operator report.partition, :>=, 0
          assert_operator report.partition, :<, producer.partition_count(topic_25)
        end
      end

      it "handles whitespace-only partition key with all partitioners" do
        @all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: topic_25,
            payload: "test payload",
            partition_key: "   ",
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout_ms: 5_000)
          assert_operator report.partition, :>=, 0
          assert_operator report.partition, :<, producer.partition_count(topic_25)
        end
      end

      it "handles newline characters in partition key with all partitioners" do
        @all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: topic_25,
            payload: "test payload",
            partition_key: "key\nwith\nnewlines",
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout_ms: 5_000)
          assert_operator report.partition, :>=, 0
          assert_operator report.partition, :<, producer.partition_count(topic_25)
        end
      end
    end

    context "debugging partitioner issues" do
      it "shows if all partitioners return 0 (indicating a problem)" do
        test_key = "debug-test-key"
        zero_count = 0

        @all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: topic_25,
            payload: "debug payload",
            partition_key: test_key,
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout_ms: 5_000)
          zero_count += 1 if report.partition == 0
        end

        assert_operator zero_count, :<, @all_partitioners.size
      end
    end
  end

  describe "#events_poll_nb_each" do
    it "does not raise when queue is empty" do
      producer.events_poll_nb_each { |_| }
    end

    it "processes delivery callbacks" do
      callback_called = false
      producer.delivery_callback = ->(_) { callback_called = true }

      handle = producer.produce(
        topic: topic,
        payload: "events_poll_nb_each test"
      )

      # Wait for message to be delivered
      handle.wait(max_wait_timeout_ms: 5_000)

      # events_poll_nb_each should process any pending callbacks
      producer.events_poll_nb_each { |_| }

      assert_equal true, callback_called
    end

    it "yields the count after each poll" do
      counts = []
      # Stub to return events, then zero
      call_count = 0
      Rdkafka::Bindings.stubs(:rd_kafka_poll_nb).with do
        call_count += 1
        true
      end.returns(1, 1, 0)

      producer.events_poll_nb_each { |count| counts << count }

      assert_equal [1, 1], counts
    end

    it "stops when block returns :stop" do
      iterations = 0
      # Stub to always return events
      Rdkafka::Bindings.stubs(:rd_kafka_poll_nb).returns(1)

      producer.events_poll_nb_each do |_count|
        iterations += 1
        :stop if iterations >= 3
      end

      assert_equal 3, iterations
    end

    context "when producer is closed" do
      before { producer.close }

      it "raises ClosedProducerError" do
        e = assert_raises(Rdkafka::ClosedProducerError) do
          producer.events_poll_nb_each { |_| }
        end
        assert_match(/events_poll_nb_each/, e.message)
      end
    end
  end

  describe "file descriptor access for fiber scheduler integration" do
    before do
      @producer = rdkafka_producer_config.producer(run_polling_thread: false)
    end

    it "enables IO events on producer queue" do
      signal_r, signal_w = IO.pipe
      producer.enable_queue_io_events(signal_w.fileno)
      signal_r.close
      signal_w.close
    end

    it "enables IO events on background queue" do
      signal_r, signal_w = IO.pipe
      producer.enable_background_queue_io_events(signal_w.fileno)
      signal_r.close
      signal_w.close
    end

    context "when producer is closed" do
      before { producer.close }

      it "raises ClosedInnerError when enabling queue_io_events" do
        signal_r, signal_w = IO.pipe
        assert_raises(Rdkafka::ClosedInnerError) do
          producer.enable_queue_io_events(signal_w.fileno)
        end
        signal_r.close
        signal_w.close
      end

      it "raises ClosedInnerError when enabling background_queue_io_events" do
        signal_r, signal_w = IO.pipe
        assert_raises(Rdkafka::ClosedInnerError) do
          producer.enable_background_queue_io_events(signal_w.fileno)
        end
        signal_r.close
        signal_w.close
      end
    end
  end
end
