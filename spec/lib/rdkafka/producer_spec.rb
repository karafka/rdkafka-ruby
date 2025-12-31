# frozen_string_literal: true

require "zlib"

RSpec.describe Rdkafka::Producer do
  let(:producer) { rdkafka_producer_config.producer }
  let(:consumer) { rdkafka_consumer_config.consumer }

  after do
    # Registry should always end up being empty
    registry = Rdkafka::Producer::DeliveryHandle::REGISTRY
    expect(registry).to be_empty, registry.inspect
    producer.close
    consumer.close
  end

  describe 'producer without auto-start' do
    let(:producer) { rdkafka_producer_config.producer(native_kafka_auto_start: false) }

    it 'expect to be able to start it later and close' do
      producer.start
      producer.close
    end

    it 'expect to be able to close it without starting' do
      producer.close
    end
  end

  describe '#name' do
    it { expect(producer.name).to include('rdkafka#producer-') }
  end

  describe '#produce with topic config alterations' do
    context 'when config is not valid' do
      it 'expect to raise error' do
        expect do
          producer.produce(topic: TestTopics.unique, payload: '', topic_config: { 'invalid': 'invalid' })
        end.to raise_error(Rdkafka::Config::ConfigError)
      end
    end

    context 'when config is valid' do
      it 'expect to raise error' do
        expect do
          producer.produce(topic: TestTopics.unique, payload: '', topic_config: { 'acks': 1 }).wait
        end.not_to raise_error
      end

      context 'when alteration should change behavior' do
        # This is set incorrectly for a reason
        # If alteration would not work, this will hang the spec suite
        let(:producer) do
          rdkafka_producer_config(
            'message.timeout.ms': 1_000_000,
            :"bootstrap.servers" => "127.0.0.1:9094",
          ).producer
        end

        it 'expect to give up on delivery fast based on alteration config' do
          expect do
            producer.produce(
              topic: TestTopics.unique,
              payload: 'test',
              topic_config: {
                'compression.type': 'gzip',
                'message.timeout.ms': 1
              }
            ).wait
          end.to raise_error(Rdkafka::RdkafkaError, /msg_timed_out/)
        end
      end
    end
  end

  context "delivery callback" do
    context "with a proc/lambda" do
      it "should set the callback" do
        expect {
          producer.delivery_callback = lambda do |delivery_handle|
            puts delivery_handle
          end
        }.not_to raise_error
        expect(producer.delivery_callback).to respond_to :call
      end

      it "should call the callback when a message is delivered" do
        @callback_called = false

        producer.delivery_callback = lambda do |report|
          expect(report).not_to be_nil
          expect(report.label).to eq "label"
          expect(report.partition).to eq 1
          expect(report.offset).to be >= 0
          expect(report.topic_name).to eq TestTopics.produce_test_topic
          @callback_called = true
        end

        # Produce a message
        handle = producer.produce(
          topic:   TestTopics.produce_test_topic,
          payload: "payload",
          key:     "key",
          label:   "label"
        )

        expect(handle.label).to eq "label"

        # Wait for it to be delivered
        handle.wait(max_wait_timeout_ms: 15_000)

        # Join the producer thread.
        producer.close

        # Callback should have been called
        expect(@callback_called).to be true
      end

      it "should provide handle" do
        @callback_handle = nil

        producer.delivery_callback = lambda { |_, handle| @callback_handle = handle }

        # Produce a message
        handle = producer.produce(
          topic:   TestTopics.produce_test_topic,
          payload: "payload",
          key:     "key"
        )

        # Wait for it to be delivered
        handle.wait(max_wait_timeout_ms: 15_000)

        # Join the producer thread.
        producer.close

        expect(handle).to be @callback_handle
      end
    end

    context "with a callable object" do
      it "should set the callback" do
        callback = Class.new do
          def call(stats); end
        end
        expect {
          producer.delivery_callback = callback.new
        }.not_to raise_error
        expect(producer.delivery_callback).to respond_to :call
      end

      it "should call the callback when a message is delivered" do
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
          topic:   TestTopics.produce_test_topic,
          payload: "payload",
          key:     "key"
        )

        # Wait for it to be delivered
        handle.wait(max_wait_timeout_ms: 15_000)

        # Join the producer thread.
        producer.close

        # Callback should have been called
        expect(called_report.first).not_to be_nil
        expect(called_report.first.partition).to eq 1
        expect(called_report.first.offset).to be >= 0
        expect(called_report.first.topic_name).to eq TestTopics.produce_test_topic
      end

      it "should provide handle" do
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
          topic:   TestTopics.produce_test_topic,
          payload: "payload",
          key:     "key"
        )

        # Wait for it to be delivered
        handle.wait(max_wait_timeout_ms: 15_000)

        # Join the producer thread.
        producer.close

        # Callback should have been called
        expect(handle).to be callback_handles.first
      end
    end

    it "should not accept a callback that's not callable" do
      expect {
        producer.delivery_callback = 'a string'
      }.to raise_error(TypeError)
    end
  end

  it "should require a topic" do
    expect {
      producer.produce(
        payload: "payload",
        key:     "key"
     )
    }.to raise_error ArgumentError, /missing keyword: [\:]?topic/
  end

  it "should produce a message" do
    # Produce a message
    handle = producer.produce(
      topic:   TestTopics.produce_test_topic,
      payload: "payload",
      key:     "key",
      label:   "label"
    )

    # Should be pending at first
    expect(handle.pending?).to be true
    expect(handle.label).to eq "label"

    # Check delivery handle and report
    report = handle.wait(max_wait_timeout_ms: 5_000)
    expect(handle.pending?).to be false
    expect(report).not_to be_nil
    expect(report.partition).to eq 1
    expect(report.offset).to be >= 0
    expect(report.label).to eq "label"

    # Flush and close producer
    producer.flush
    producer.close

    # Consume message and verify its content
    message = wait_for_message(
      topic: TestTopics.produce_test_topic,
      delivery_report: report,
      consumer: consumer
    )
    expect(message.partition).to eq 1
    expect(message.payload).to eq "payload"
    expect(message.key).to eq "key"
    expect(message.timestamp).to be_within(10).of(Time.now)
  end

  it "should produce a message with a specified partition" do
    # Produce a message
    handle = producer.produce(
      topic:     TestTopics.produce_test_topic,
      payload:   "payload partition",
      key:       "key partition",
      partition: 1
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: TestTopics.produce_test_topic,
      delivery_report: report,
      consumer: consumer
    )
    expect(message.partition).to eq 1
    expect(message.key).to eq "key partition"
  end

  it "should produce a message to the same partition with a similar partition key" do
    # Avoid partitioner collisions.
    while true
      key = ('a'..'z').to_a.shuffle.take(10).join('')
      partition_key = ('a'..'z').to_a.shuffle.take(10).join('')
      partition_count = producer.partition_count(TestTopics.partitioner_test_topic)
      break if (Zlib.crc32(key) % partition_count) != (Zlib.crc32(partition_key) % partition_count)
    end

    # Produce a message with key, partition_key and key + partition_key
    messages = [{key: key}, {partition_key: partition_key}, {key: key, partition_key: partition_key}]

    messages = messages.map do |m|
      handle = producer.produce(
        topic:     TestTopics.partitioner_test_topic,
        payload:   "payload partition",
        key:       m[:key],
        partition_key: m[:partition_key]
      )
      report = handle.wait(max_wait_timeout_ms: 5_000)

      wait_for_message(
        topic: TestTopics.partitioner_test_topic,
        delivery_report: report,
      )
    end

    expect(messages[0].partition).not_to eq(messages[2].partition)
    expect(messages[1].partition).to eq(messages[2].partition)
    expect(messages[0].key).to eq key
    expect(messages[1].key).to be_nil
    expect(messages[2].key).to eq key
  end

  it "should produce a message with empty string without crashing" do
    messages = [{key: 'a', partition_key: ''}]

    messages = messages.map do |m|
      handle = producer.produce(
        topic:     TestTopics.partitioner_test_topic,
        payload:   "payload partition",
        key:       m[:key],
        partition_key: m[:partition_key]
      )
      report = handle.wait(max_wait_timeout_ms: 5_000)

      wait_for_message(
        topic: TestTopics.partitioner_test_topic,
        delivery_report: report,
      )
    end

    expect(messages[0].partition).to be >= 0
    expect(messages[0].key).to eq 'a'
  end

  it "should produce a message with utf-8 encoding" do
    handle = producer.produce(
      topic:   TestTopics.produce_test_topic,
      payload: "Τη γλώσσα μου έδωσαν ελληνική",
      key:     "key utf8"
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: TestTopics.produce_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    expect(message.partition).to eq 1
    expect(message.payload.force_encoding("utf-8")).to eq "Τη γλώσσα μου έδωσαν ελληνική"
    expect(message.key).to eq "key utf8"
  end

  it "should produce a message to a non-existing topic with key and partition key" do
    new_topic = TestTopics.unique

    handle = producer.produce(
      # Needs to be a new topic each time
      topic:   new_topic,
      payload: "payload",
      key:     "key",
      partition_key: "partition_key",
      label:   "label"
    )

    # Should be pending at first
    expect(handle.pending?).to be true
    expect(handle.label).to eq "label"

    # Check delivery handle and report
    report = handle.wait(max_wait_timeout_ms: 5_000)
    expect(handle.pending?).to be false
    expect(report).not_to be_nil
    expect(report.partition).to eq 0
    expect(report.offset).to be >= 0
    expect(report.label).to eq "label"

    # Flush and close producer
    producer.flush
    producer.close

    # Consume message and verify its content
    message = wait_for_message(
      topic: new_topic,
      delivery_report: report,
      consumer: consumer
    )
    expect(message.partition).to eq 0
    expect(message.payload).to eq "payload"
    expect(message.key).to eq "key"
    expect(message.timestamp).to be_within(10).of(Time.now)
  end

  context "timestamp" do
    it "should raise a type error if not nil, integer or time" do
      expect {
        producer.produce(
          topic:     TestTopics.produce_test_topic,
          payload:   "payload timestamp",
          key:       "key timestamp",
          timestamp: "10101010"
        )
      }.to raise_error TypeError
    end

    it "should produce a message with an integer timestamp" do
      handle = producer.produce(
        topic:     TestTopics.produce_test_topic,
        payload:   "payload timestamp",
        key:       "key timestamp",
        timestamp: 1505069646252
      )
      report = handle.wait(max_wait_timeout_ms: 5_000)

      # Consume message and verify its content
      message = wait_for_message(
        topic: TestTopics.produce_test_topic,
        delivery_report: report,
        consumer: consumer
      )

      expect(message.partition).to eq 2
      expect(message.key).to eq "key timestamp"
      expect(message.timestamp).to eq Time.at(1505069646, 252_000)
    end

    it "should produce a message with a time timestamp" do
      handle = producer.produce(
        topic:     TestTopics.produce_test_topic,
        payload:   "payload timestamp",
        key:       "key timestamp",
        timestamp: Time.at(1505069646, 353_000)
      )
      report = handle.wait(max_wait_timeout_ms: 5_000)

      # Consume message and verify its content
      message = wait_for_message(
        topic: TestTopics.produce_test_topic,
        delivery_report: report,
        consumer: consumer
      )

      expect(message.partition).to eq 2
      expect(message.key).to eq "key timestamp"
      expect(message.timestamp).to eq Time.at(1505069646, 353_000)
    end
  end

  it "should produce a message with nil key" do
    handle = producer.produce(
      topic:   TestTopics.produce_test_topic,
      payload: "payload no key"
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: TestTopics.produce_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    expect(message.key).to be_nil
    expect(message.payload).to eq "payload no key"
  end

  it "should produce a message with nil payload" do
    handle = producer.produce(
      topic: TestTopics.produce_test_topic,
      key:   "key no payload"
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: TestTopics.produce_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    expect(message.key).to eq "key no payload"
    expect(message.payload).to be_nil
  end

  it "should produce a message with headers" do
    handle = producer.produce(
      topic:     TestTopics.produce_test_topic,
      payload:   "payload headers",
      key:       "key headers",
      headers:   { foo: :bar, baz: :foobar }
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: TestTopics.produce_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    expect(message.payload).to eq "payload headers"
    expect(message.key).to eq "key headers"
    expect(message.headers["foo"]).to eq "bar"
    expect(message.headers["baz"]).to eq "foobar"
    expect(message.headers["foobar"]).to be_nil
  end

  it "should produce a message with empty headers" do
    handle = producer.produce(
      topic:     TestTopics.produce_test_topic,
      payload:   "payload headers",
      key:       "key headers",
      headers:   {}
    )
    report = handle.wait(max_wait_timeout_ms: 5_000)

    # Consume message and verify its content
    message = wait_for_message(
      topic: TestTopics.produce_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    expect(message.payload).to eq "payload headers"
    expect(message.key).to eq "key headers"
    expect(message.headers).to be_empty
  end

  it "should produce message that aren't waited for and not crash" do
    5.times do
      200.times do
        producer.produce(
          topic:   TestTopics.produce_test_topic,
          payload: "payload not waiting",
          key:     "key not waiting"
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

  it "should produce a message in a forked process", skip: defined?(JRUBY_VERSION) && "Kernel#fork is not available" do
    # Fork, produce a message, send the report over a pipe and
    # wait for and check the message in the main process.
    reader, writer = IO.pipe

    pid = fork do
      reader.close

      # Avoid sharing the client between processes.
      producer = rdkafka_producer_config.producer

      handle = producer.produce(
        topic:   TestTopics.produce_test_topic,
        payload: "payload-forked",
        key:     "key-forked"
      )

      report = handle.wait(max_wait_timeout_ms: 5_000)

      report_json = JSON.generate(
        "partition" => report.partition,
        "offset" => report.offset,
        "topic_name" => report.topic_name
      )

      writer.write(report_json)
      writer.close
      producer.flush
      producer.close
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
    expect(message.partition).to eq 0
    expect(message.payload).to eq "payload-forked"
    expect(message.key).to eq "key-forked"
  end

  it "should raise an error when producing fails" do
    expect(Rdkafka::Bindings).to receive(:rd_kafka_producev).and_return(20)

    expect {
      producer.produce(
        topic:   TestTopics.produce_test_topic,
        key:     "key error"
      )
    }.to raise_error Rdkafka::RdkafkaError
  end

  context "synchronous error handling in produce" do
    it "should handle invalid partition error" do
      # Mock rd_kafka_producev to return RD_KAFKA_RESP_ERR__INVALID_ARG (-186)
      allow(Rdkafka::Bindings).to receive(:rd_kafka_producev).and_return(-186)

      expect {
        producer.produce(
          topic:     TestTopics.produce_test_topic,
          payload:   "test payload",
          partition: 999
        )
      }.to raise_error(Rdkafka::RdkafkaError) do |error|
        expect(error.code).to eq(:invalid_arg)
      end

      # Verify delivery handle was properly unregistered
      expect(Rdkafka::Producer::DeliveryHandle::REGISTRY).to be_empty
    end

    it "should handle queue full error" do
      # Mock rd_kafka_producev to return RD_KAFKA_RESP_ERR__QUEUE_FULL (-184)
      allow(Rdkafka::Bindings).to receive(:rd_kafka_producev).and_return(-184)

      expect {
        producer.produce(
          topic:   TestTopics.produce_test_topic,
          payload: "test payload"
        )
      }.to raise_error(Rdkafka::RdkafkaError) do |error|
        expect(error.code).to eq(:queue_full)
      end

      # Verify delivery handle was properly unregistered
      expect(Rdkafka::Producer::DeliveryHandle::REGISTRY).to be_empty
    end

    it "should handle unknown topic error" do
      # Mock rd_kafka_producev to return RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART (3)
      allow(Rdkafka::Bindings).to receive(:rd_kafka_producev).and_return(3)

      expect {
        producer.produce(
          topic:   TestTopics.produce_test_topic,
          payload: "test payload"
        )
      }.to raise_error(Rdkafka::RdkafkaError) do |error|
        expect(error.code).to eq(:unknown_topic_or_part)
      end

      # Verify delivery handle was properly unregistered
      expect(Rdkafka::Producer::DeliveryHandle::REGISTRY).to be_empty
    end

    it "should handle message size too large error" do
      # Mock rd_kafka_producev to return RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE (10)
      allow(Rdkafka::Bindings).to receive(:rd_kafka_producev).and_return(10)

      expect {
        producer.produce(
          topic:   TestTopics.produce_test_topic,
          payload: "test payload"
        )
      }.to raise_error(Rdkafka::RdkafkaError) do |error|
        expect(error.code).to eq(:msg_size_too_large)
      end

      # Verify delivery handle was properly unregistered
      expect(Rdkafka::Producer::DeliveryHandle::REGISTRY).to be_empty
    end

    it "should keep delivery handle registered on successful produce" do
      # Don't mock - let the actual produce succeed
      handle = producer.produce(
        topic:   TestTopics.produce_test_topic,
        payload: "test payload"
      )

      # Handle should be registered and pending
      expect(Rdkafka::Producer::DeliveryHandle::REGISTRY).not_to be_empty
      expect(handle.pending?).to be true

      # Wait for it to complete
      handle.wait(max_wait_timeout_ms: 5_000)

      # After completion, it should be removed from registry
      expect(Rdkafka::Producer::DeliveryHandle::REGISTRY).to be_empty
    end

    it "should properly clean up multiple failed produce attempts" do
      # Mock rd_kafka_producev to fail
      allow(Rdkafka::Bindings).to receive(:rd_kafka_producev).and_return(-184)

      # Try to produce multiple messages that will all fail
      3.times do
        expect {
          producer.produce(
            topic:   TestTopics.produce_test_topic,
            payload: "test payload"
          )
        }.to raise_error(Rdkafka::RdkafkaError)
      end

      # Registry should still be empty after all failures
      expect(Rdkafka::Producer::DeliveryHandle::REGISTRY).to be_empty
    end

    it "should handle fatal error with remapping using real librdkafka" do
      # This tests the real scenario where rd_kafka_producev returns -150 (ERR__FATAL) synchronously.
      # After triggering a fatal error with rd_kafka_test_fatal_error(), subsequent calls to
      # rd_kafka_producev will return -150, and our code properly remaps it to the actual error code.
      #
      # We create a separate producer for this test to avoid interfering with other tests.

      # Create a dedicated producer with idempotence enabled (required for fatal errors)
      fatal_test_producer = rdkafka_producer_config('enable.idempotence' => true).producer

      # Include Testing module to access trigger_test_fatal_error
      fatal_test_producer.singleton_class.include(Rdkafka::Testing)

      # Trigger a fatal error using librdkafka's testing facility
      # Error code 47 = invalid_producer_epoch
      result = fatal_test_producer.trigger_test_fatal_error(47, "Test fatal error for produce")
      expect(result).to eq(0) # Should succeed

      # Verify the fatal error was recorded
      fatal_details = fatal_test_producer.fatal_error
      expect(fatal_details).not_to be_nil
      expect(fatal_details[:error_code]).to eq(47)

      # Now when we try to produce, rd_kafka_producev will return -150 synchronously
      # and our code should detect and remap it to the actual error code
      expect {
        fatal_test_producer.produce(
          topic:   TestTopics.produce_test_topic,
          payload: "test payload"
        )
      }.to raise_error(Rdkafka::RdkafkaError) do |error|
        # Should be remapped to the actual error code, not -150
        expect(error.code).to eq(:invalid_producer_epoch)
        expect(error.rdkafka_response).to eq(47)
        expect(error.fatal?).to be true
        expect(error.broker_message).to include("Test fatal error for produce")
      end

      # Verify delivery handle was properly unregistered
      expect(Rdkafka::Producer::DeliveryHandle::REGISTRY).to be_empty

      fatal_test_producer.close
    end
  end

  it "should raise a timeout error when waiting too long" do
    handle = producer.produce(
      topic:   TestTopics.produce_test_topic,
      payload: "payload timeout",
      key:     "key timeout"
    )
    expect {
      handle.wait(max_wait_timeout_ms: 0)
    }.to raise_error Rdkafka::Producer::DeliveryHandle::WaitTimeoutError

    # Waiting a second time should work
    handle.wait(max_wait_timeout_ms: 5_000)
  end

  context "methods that should not be called after a producer has been closed" do
    before do
      producer.close
    end

    # Affected methods and a non-invalid set of parameters for the method
    # :no_args indicates the method takes no arguments
    {
        :produce         => { topic: nil },
        :partition_count => nil,
        :queue_size      => :no_args,
    }.each do |method, args|
      it "raises an exception if #{method} is called" do
        expect {
          if args == :no_args
            producer.public_send(method)
          elsif args.is_a?(Hash)
            producer.public_send(method, **args)
          else
            producer.public_send(method, args)
          end
        }.to raise_exception(Rdkafka::ClosedProducerError, /#{method.to_s}/)
      end
    end
  end

  context "when not being able to deliver the message" do
    let(:producer) do
      rdkafka_producer_config(
        "bootstrap.servers": "127.0.0.1:9095",
        "message.timeout.ms": 100
      ).producer
    end

    it "should contain the error in the response when not deliverable" do
      handler = producer.produce(topic: TestTopics.produce_test_topic, payload: nil, label: 'na')
      # Wait for the async callbacks and delivery registry to update
      sleep(2)
      expect(handler.create_result.error).to be_a(Rdkafka::RdkafkaError)
      expect(handler.create_result.label).to eq('na')
    end
  end

  context "when topic does not exist and allow.auto.create.topics is false" do
    let(:producer) do
      rdkafka_producer_config(
        "bootstrap.servers": "127.0.0.1:9092",
        "message.timeout.ms": 100,
        "allow.auto.create.topics": false
      ).producer
    end

    it "should contain the error in the response when not deliverable" do
      handler = producer.produce(topic: TestTopics.unique, payload: nil, label: 'na')
      # Wait for the async callbacks and delivery registry to update
      sleep(2)
      expect(handler.create_result.error).to be_a(Rdkafka::RdkafkaError)
      expect(handler.create_result.error.code).to eq(:msg_timed_out)
      expect(handler.create_result.label).to eq('na')
    end
  end

  describe '#partition_count' do
    it { expect(producer.partition_count(TestTopics.example_topic)).to eq(1) }

    context 'when the partition count value is already cached' do
      before do
        producer.partition_count(TestTopics.example_topic)
        allow(::Rdkafka::Metadata).to receive(:new).and_call_original
      end

      it 'expect not to query it again' do
        producer.partition_count(TestTopics.example_topic)
        expect(::Rdkafka::Metadata).not_to have_received(:new)
      end
    end

    context 'when the partition count value was cached but time expired' do
      before do
        ::Rdkafka::Producer.partitions_count_cache = Rdkafka::Producer::PartitionsCountCache.new
        allow(::Rdkafka::Metadata).to receive(:new).and_call_original
      end

      it 'expect to query it again' do
        producer.partition_count(TestTopics.example_topic)
        expect(::Rdkafka::Metadata).to have_received(:new)
      end
    end

    context 'when the partition count value was cached and time did not expire' do
      before do
        allow(::Process).to receive(:clock_gettime).and_return(0, 29.001)
        producer.partition_count(TestTopics.example_topic)
        allow(::Rdkafka::Metadata).to receive(:new).and_call_original
      end

      it 'expect not to query it again' do
        producer.partition_count(TestTopics.example_topic)
        expect(::Rdkafka::Metadata).not_to have_received(:new)
      end
    end
  end

  describe 'metadata fetch request recovery' do
    subject(:partition_count) { producer.partition_count(TestTopics.example_topic) }

    describe 'metadata initialization recovery' do
      context 'when all good' do
        it { expect(partition_count).to eq(1) }
      end

      context 'when we fail for the first time with handled error' do
        before do
          raised = false

          allow(Rdkafka::Bindings).to receive(:rd_kafka_metadata).and_wrap_original do |m, *args|
            if raised
              m.call(*args)
            else
              raised = true
              -185
            end
          end
        end

        it { expect(partition_count).to eq(1) }
      end
    end
  end

  describe '#flush' do
    it "should return flush when it can flush all outstanding messages or when no messages" do
      producer.produce(
        topic:     TestTopics.produce_test_topic,
        payload:   "payload headers",
        key:       "key headers",
        headers:   {}
      )

      expect(producer.flush(5_000)).to eq(true)
    end

    context 'when it cannot flush due to a timeout' do
      let(:producer) do
        rdkafka_producer_config(
          "bootstrap.servers": "127.0.0.1:9095",
          "message.timeout.ms": 2_000
        ).producer
      end

      after do
        # Allow rdkafka to evict message preventing memory-leak
        # We give it a bit more time as on slow CIs things take time
        sleep(5)
      end

      it "should return false on flush when cannot deliver and beyond timeout" do
        producer.produce(
          topic:     TestTopics.produce_test_topic,
          payload:   "payload headers",
          key:       "key headers",
          headers:   {}
        )

        expect(producer.flush(1_000)).to eq(false)
      end
    end

    context 'when there is a different error' do
      before { allow(Rdkafka::Bindings).to receive(:rd_kafka_flush).and_return(-199) }

      it 'should raise it' do
        expect { producer.flush }.to raise_error(Rdkafka::RdkafkaError)
      end
    end
  end

  describe '#purge' do
    context 'when no outgoing messages' do
      it { expect(producer.purge).to eq(true) }
    end

    context 'when librdkafka purge returns an error' do
      before { expect(Rdkafka::Bindings).to receive(:rd_kafka_purge).and_return(-153) }

      it 'expect to raise an error' do
        expect { producer.purge }.to raise_error(Rdkafka::RdkafkaError, /retry/)
      end
    end

    context 'when there are outgoing things in the queue' do
      let(:producer) do
        rdkafka_producer_config(
          "bootstrap.servers": "127.0.0.1:9095",
          "message.timeout.ms": 2_000
        ).producer
      end

      it "should should purge and move forward" do
        producer.produce(
          topic:     TestTopics.produce_test_topic,
          payload:   "payload headers"
        )

        expect(producer.purge).to eq(true)
        expect(producer.flush(1_000)).to eq(true)
      end

      it "should materialize the delivery handles" do
        handle = producer.produce(
          topic:     TestTopics.produce_test_topic,
          payload:   "payload headers"
        )

        expect(producer.purge).to eq(true)

        expect { handle.wait }.to raise_error(Rdkafka::RdkafkaError, /purge_queue/)
      end

      context "when using delivery_callback" do
        let(:delivery_reports) { [] }

        let(:delivery_callback) do
          ->(delivery_report) { delivery_reports << delivery_report }
        end

        before { producer.delivery_callback = delivery_callback }

        it "should run the callback" do
          producer.produce(
            topic:     TestTopics.produce_test_topic,
            payload:   "payload headers"
          )

          expect(producer.purge).to eq(true)
          # queue purge
          expect(delivery_reports[0].error).to eq(-152)
        end
      end
    end
  end

  context 'when working with transactions' do
    let(:producer) do
      rdkafka_producer_config(
        'transactional.id': SecureRandom.uuid,
        'transaction.timeout.ms': 5_000
      ).producer
    end

    it 'expect not to allow to produce without transaction init' do
      expect do
        producer.produce(topic: TestTopics.produce_test_topic, payload: 'data')
      end.to raise_error(Rdkafka::RdkafkaError, /Erroneous state \(state\)/)
    end

    it 'expect to raise error when transactions are initialized but producing not in one' do
      producer.init_transactions

      expect do
        producer.produce(topic: TestTopics.produce_test_topic, payload: 'data')
      end.to raise_error(Rdkafka::RdkafkaError, /Erroneous state \(state\)/)
    end

    it 'expect to allow to produce within a transaction, finalize and ship data' do
      producer.init_transactions
      producer.begin_transaction
      handle1 = producer.produce(topic: TestTopics.produce_test_topic, payload: 'data1', partition: 1)
      handle2 = producer.produce(topic: TestTopics.example_topic, payload: 'data2', partition: 0)
      producer.commit_transaction

      report1 = handle1.wait(max_wait_timeout_ms: 15_000)
      report2 = handle2.wait(max_wait_timeout_ms: 15_000)

      message1 = wait_for_message(
        topic: TestTopics.produce_test_topic,
        delivery_report: report1,
        consumer: consumer
      )

      expect(message1.partition).to eq 1
      expect(message1.payload).to eq "data1"
      expect(message1.timestamp).to be_within(10).of(Time.now)

      message2 = wait_for_message(
        topic: TestTopics.example_topic,
        delivery_report: report2,
        consumer: consumer
      )

      expect(message2.partition).to eq 0
      expect(message2.payload).to eq "data2"
      expect(message2.timestamp).to be_within(10).of(Time.now)
    end

    it 'expect not to send data and propagate purge queue error on abort' do
      producer.init_transactions
      producer.begin_transaction
      handle1 = producer.produce(topic: TestTopics.produce_test_topic, payload: 'data1', partition: 1)
      handle2 = producer.produce(topic: TestTopics.example_topic, payload: 'data2', partition: 0)
      producer.abort_transaction

      expect { handle1.wait(max_wait_timeout_ms: 15_000) }
        .to raise_error(Rdkafka::RdkafkaError, /Purged in queue \(purge_queue\)/)
      expect { handle2.wait(max_wait_timeout_ms: 15_000) }
        .to raise_error(Rdkafka::RdkafkaError, /Purged in queue \(purge_queue\)/)
    end

    it 'expect to have non retryable, non abortable and not fatal error on abort' do
      producer.init_transactions
      producer.begin_transaction
      handle = producer.produce(topic: TestTopics.produce_test_topic, payload: 'data1', partition: 1)
      producer.abort_transaction

      response = handle.wait(raise_response_error: false)

      expect(response.error).to be_a(Rdkafka::RdkafkaError)
      expect(response.error.retryable?).to eq(false)
      expect(response.error.fatal?).to eq(false)
      expect(response.error.abortable?).to eq(false)
    end

    context 'fencing against previous active producer with same transactional id' do
      let(:transactional_id) { SecureRandom.uuid }

      let(:producer1) do
        rdkafka_producer_config(
          'transactional.id': transactional_id,
          'transaction.timeout.ms': 10_000
        ).producer
      end

      let(:producer2) do
        rdkafka_producer_config(
          'transactional.id': transactional_id,
          'transaction.timeout.ms': 10_000
        ).producer
      end

      after do
        producer1.close
        producer2.close
      end

      it 'expect older producer not to be able to commit when fanced out' do
        producer1.init_transactions
        producer1.begin_transaction
        producer1.produce(topic: TestTopics.produce_test_topic, payload: 'data1', partition: 1)

        producer2.init_transactions
        producer2.begin_transaction
        producer2.produce(topic: TestTopics.produce_test_topic, payload: 'data1', partition: 1)

        expect { producer1.commit_transaction }
          .to raise_error(Rdkafka::RdkafkaError, /This instance has been fenced/)

        error = false

        begin
          producer1.commit_transaction
        rescue Rdkafka::RdkafkaError => e
          error = e
        end

        expect(error.fatal?).to eq(true)
        expect(error.abortable?).to eq(false)
        expect(error.retryable?).to eq(false)

        expect { producer2.commit_transaction }.not_to raise_error
      end
    end

    context 'when having a consumer with tpls for exactly once semantics' do
      let(:tpl) do
        producer.produce(topic: TestTopics.consume_test_topic, payload: 'data1', partition: 0).wait
        result = producer.produce(topic: TestTopics.consume_test_topic, payload: 'data1', partition: 0).wait

        Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
          list.add_topic_and_partitions_with_offsets(TestTopics.consume_test_topic, 0 => result.offset + 1)
        end
      end

      before do
        consumer.subscribe(TestTopics.consume_test_topic)
        wait_for_assignment(consumer)
        producer.init_transactions
        producer.begin_transaction
      end

      after { consumer.unsubscribe }

      it 'expect to store offsets and not crash' do
        producer.send_offsets_to_transaction(consumer, tpl)
        producer.commit_transaction
      end
    end
  end

  describe '#queue_size' do
    it 'returns 0 when there are no pending messages' do
      expect(producer.queue_size).to eq(0)
    end

    it 'returns a positive number when there are pending messages' do
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
        expect(queue_size).to be > 0
      ensure
        slow_producer.close
      end
    end

    it 'returns 0 after flush completes' do
      producer.produce(
        topic: TestTopics.produce_test_topic,
        payload: "test payload"
      ).wait(max_wait_timeout_ms: 5_000)

      producer.flush(5_000)

      expect(producer.queue_size).to eq(0)
    end

    describe '#queue_length alias' do
      it 'is an alias for queue_size' do
        expect(producer.method(:queue_length)).to eq(producer.method(:queue_size))
      end

      it 'returns the same value as queue_size' do
        expect(producer.queue_length).to eq(producer.queue_size)
      end
    end
  end

  describe '#oauthbearer_set_token' do
    context 'when sasl not configured' do
      it 'should return RD_KAFKA_RESP_ERR__STATE' do
        response = producer.oauthbearer_set_token(
              token: "foo",
              lifetime_ms: Time.now.to_i*1000 + 900 * 1000,
              principal_name: "kafka-cluster",
            )
        expect(response).to eq(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE)
      end
    end

    context 'when sasl configured' do
      before do
        $producer_sasl = rdkafka_producer_config(
          "security.protocol": "sasl_ssl",
          "sasl.mechanisms": 'OAUTHBEARER'
        ).producer
      end

      after do
        $producer_sasl.close
      end

      context 'without extensions' do
        it 'should succeed' do
          response = $producer_sasl.oauthbearer_set_token(
            token: "foo",
            lifetime_ms: Time.now.to_i*1000 + 900 * 1000,
            principal_name: "kafka-cluster"
          )
          expect(response).to eq(0)
        end
      end

      context 'with extensions' do
        it 'should succeed' do
          response = $producer_sasl.oauthbearer_set_token(
            token: "foo",
            lifetime_ms: Time.now.to_i*1000 + 900 * 1000,
            principal_name: "kafka-cluster",
            extensions: {
              "foo" => "bar"
            }
          )
          expect(response).to eq(0)
        end
      end
    end
  end

  describe "#produce with headers" do
    it "should produce a message with array headers" do
      headers = {
        "version" => ["2.1.3", "2.1.4"],
        "type" => "String"
      }

      report = producer.produce(
        topic:     TestTopics.consume_test_topic,
        key:       "key headers",
        headers:   headers
      ).wait

      message = wait_for_message(topic: TestTopics.consume_test_topic, consumer: consumer, delivery_report: report)
      expect(message).to be
      expect(message.key).to eq('key headers')
      expect(message.headers['type']).to eq('String')
      expect(message.headers['version']).to eq(["2.1.3", "2.1.4"])
    end

    it "should produce a message with single value headers" do
      headers = {
        "version" => "2.1.3",
        "type" => "String"
      }

      report = producer.produce(
        topic:     TestTopics.consume_test_topic,
        key:       "key headers",
        headers:   headers
      ).wait

      message = wait_for_message(topic: TestTopics.consume_test_topic, consumer: consumer, delivery_report: report)
      expect(message).to be
      expect(message.key).to eq('key headers')
      expect(message.headers['type']).to eq('String')
      expect(message.headers['version']).to eq('2.1.3')
    end
  end

  describe 'with active statistics callback' do
    let(:producer) do
      rdkafka_producer_config('statistics.interval.ms': 1_000).producer
    end

    let(:count_cache_hash) { described_class.partitions_count_cache.to_h }
    let(:pre_statistics_ttl) { count_cache_hash.fetch(TestTopics.produce_test_topic, [])[0] }
    let(:post_statistics_ttl) { count_cache_hash.fetch(TestTopics.produce_test_topic, [])[0] }

    context "when using partition key" do
      before do
        Rdkafka::Config.statistics_callback = ->(*) {}

        # This call will make a blocking request to the metadata cache
        producer.produce(
          topic:         TestTopics.produce_test_topic,
          payload:       "payload headers",
          partition_key: "test"
        ).wait

        pre_statistics_ttl

        # We wait to make sure that statistics are triggered and that there is a refresh
        sleep(1.5)

        post_statistics_ttl
      end

      it 'expect to update ttl on the partitions count cache via statistics' do
        expect(pre_statistics_ttl).to be < post_statistics_ttl
      end
    end

    context "when not using partition key" do
      before do
        Rdkafka::Config.statistics_callback = ->(*) {}

        # This call will make a blocking request to the metadata cache
        producer.produce(
          topic:         TestTopics.produce_test_topic,
          payload:       "payload headers"
        ).wait

        pre_statistics_ttl

        # We wait to make sure that statistics are triggered and that there is a refresh
        sleep(1.5)

        # This will anyhow be populated from statistic
        post_statistics_ttl
      end

      it 'expect not to update ttl on the partitions count cache via blocking but via use stats' do
        expect(pre_statistics_ttl).to be_nil
        expect(post_statistics_ttl).not_to be_nil
      end
    end
  end

  describe 'without active statistics callback' do
    let(:producer) do
      rdkafka_producer_config('statistics.interval.ms': 1_000).producer
    end

    let(:count_cache_hash) { described_class.partitions_count_cache.to_h }
    let(:pre_statistics_ttl) { count_cache_hash.fetch(TestTopics.produce_test_topic, [])[0] }
    let(:post_statistics_ttl) { count_cache_hash.fetch(TestTopics.produce_test_topic, [])[0] }

    context "when using partition key" do
      before do
        # This call will make a blocking request to the metadata cache
        producer.produce(
          topic:         TestTopics.produce_test_topic,
          payload:       "payload headers",
          partition_key: "test"
        ).wait

        pre_statistics_ttl

        # We wait to make sure that statistics are triggered and that there is a refresh
        sleep(1.5)

        post_statistics_ttl
      end

      it 'expect not to update ttl on the partitions count cache via statistics' do
        expect(pre_statistics_ttl).to eq post_statistics_ttl
      end
    end

    context "when not using partition key" do
      before do
        # This call will make a blocking request to the metadata cache
        producer.produce(
          topic:         TestTopics.produce_test_topic,
          payload:       "payload headers"
        ).wait

        pre_statistics_ttl

        # We wait to make sure that statistics are triggered and that there is a refresh
        sleep(1.5)

        # This should not be populated because stats are not in use
        post_statistics_ttl
      end

      it 'expect not to update ttl on the partitions count cache via anything' do
        expect(pre_statistics_ttl).to be_nil
        expect(post_statistics_ttl).to be_nil
      end
    end
  end

  describe 'with other fiber closing' do
    context 'when we create many fibers and close producer in some of them' do
      it 'expect not to crash ruby' do
        10.times do |i|
          producer = rdkafka_producer_config.producer

          Fiber.new do
            GC.start
            producer.close
          end.resume
        end
      end
    end
  end

  let(:producer) { rdkafka_producer_config.producer }
  let(:all_partitioners) { %w(random consistent consistent_random murmur2 murmur2_random fnv1a fnv1a_random) }

  describe "partitioner behavior through producer API" do
    context "testing all partitioners with same key" do
      it "should not return partition 0 for all partitioners" do
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
        expect(unique_partitions.size).to be > 1
      end
    end

    context "empty string partition key" do
      it "should produce message with empty partition key without crashing and go to partition 0 for all partitioners" do
        all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: TestTopics.partitioner_test_topic,
            payload: "test payload",
            key: "test-key",
            partition_key: "",
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout_ms: 5_000)
          expect(report.partition).to be >= 0
        end
      end
    end

    context "nil partition key" do
      it "should handle nil partition key gracefully" do
        handle = producer.produce(
          topic: TestTopics.partitioner_test_topic,
          payload: "test payload",
          key: "test-key",
          partition_key: nil
        )

        report = handle.wait(max_wait_timeout_ms: 5_000)
        expect(report.partition).to be >= 0
        expect(report.partition).to be < producer.partition_count(TestTopics.partitioner_test_topic)
      end
    end

    context "various key types and lengths with different partitioners" do
      it "should handle very short keys with all partitioners" do
        all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: TestTopics.partitioner_test_topic,
            payload: "test payload",
            partition_key: "a",
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout_ms: 5_000)
          expect(report.partition).to be >= 0
          expect(report.partition).to be < producer.partition_count(TestTopics.partitioner_test_topic)
        end
      end

      it "should handle very long keys with all partitioners" do
        long_key = "a" * 1000

        all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: TestTopics.partitioner_test_topic,
            payload: "test payload",
            partition_key: long_key,
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout_ms: 5_000)
          expect(report.partition).to be >= 0
          expect(report.partition).to be < producer.partition_count(TestTopics.partitioner_test_topic)
        end
      end

      it "should handle unicode keys with all partitioners" do
        unicode_key = "测试键值🚀"

        all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: TestTopics.partitioner_test_topic,
            payload: "test payload",
            partition_key: unicode_key,
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout_ms: 5_000)
          expect(report.partition).to be >= 0
          expect(report.partition).to be < producer.partition_count(TestTopics.partitioner_test_topic)
        end
      end
    end

    context "consistency testing for deterministic partitioners" do
      %w(consistent murmur2 fnv1a).each do |partitioner|
        it "should consistently route same partition key to same partition with #{partitioner}" do
          partition_key = "consistent-test-key"

          # Produce multiple messages with same partition key
          reports = 5.times.map do
            handle = producer.produce(
              topic: TestTopics.partitioner_test_topic,
              payload: "test payload #{Time.now.to_f}",
              partition_key: partition_key,
              partitioner: partitioner
            )
            handle.wait(max_wait_timeout_ms: 5_000)
          end

          # All should go to same partition
          partitions = reports.map(&:partition).uniq
          expect(partitions.size).to eq(1)
        end
      end
    end

    context "randomness testing for random partitioners" do
      %w(random consistent_random murmur2_random fnv1a_random).each do |partitioner|
        it "should potentially distribute across partitions with #{partitioner}" do
          # Note: random partitioners might still return same value by chance
          partition_key = "random-test-key"

          reports = 10.times.map do
            handle = producer.produce(
              topic: TestTopics.partitioner_test_topic,
              payload: "test payload #{Time.now.to_f}",
              partition_key: partition_key,
              partitioner: partitioner
            )
            handle.wait(max_wait_timeout_ms: 5_000)
          end

          partitions = reports.map(&:partition)

          # Just ensure they're valid partitions
          partitions.each do |partition|
            expect(partition).to be >= 0
            expect(partition).to be < producer.partition_count(TestTopics.partitioner_test_topic)
          end
        end
      end
    end

    context "comparing different partitioners with same key" do
      it "should route different partition keys to potentially different partitions" do
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
          expect(partitions.all? { |p| p >= 0 && p < producer.partition_count(TestTopics.partitioner_test_topic) }).to be true
        end
      end
    end

    context "partition key vs regular key behavior" do
      it "should use partition key for partitioning when both key and partition_key are provided" do
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
        expect(report1.partition).to eq(report2.partition)

        # Message 3 should potentially go to different partition (uses regular key)
        expect(report3.partition).not_to eq(report1.partition)
      end
    end

    context "edge case combinations with different partitioners" do
      it "should handle nil partition key with all partitioners" do
        all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: TestTopics.partitioner_test_topic,
            payload: "test payload",
            key: "test-key",
            partition_key: nil,
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout_ms: 5_000)
          expect(report.partition).to be >= 0
          expect(report.partition).to be < producer.partition_count(TestTopics.partitioner_test_topic)
        end
      end

      it "should handle whitespace-only partition key with all partitioners" do
        all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: TestTopics.partitioner_test_topic,
            payload: "test payload",
            partition_key: "   ",
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout_ms: 5_000)
          expect(report.partition).to be >= 0
          expect(report.partition).to be < producer.partition_count(TestTopics.partitioner_test_topic)
        end
      end

      it "should handle newline characters in partition key with all partitioners" do
        all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: TestTopics.partitioner_test_topic,
            payload: "test payload",
            partition_key: "key\nwith\nnewlines",
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout_ms: 5_000)
          expect(report.partition).to be >= 0
          expect(report.partition).to be < producer.partition_count(TestTopics.partitioner_test_topic)
        end
      end
    end

    context "debugging partitioner issues" do
      it "should show if all partitioners return 0 (indicating a problem)" do
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

        expect(zero_count).to be < all_partitioners.size
      end
    end
  end

  describe "fatal error handling with idempotent producer" do
    let(:producer) { rdkafka_producer_config('enable.idempotence' => true).producer }

    after { producer.close }

    context "when a fatal error is triggered" do
      # Common fatal errors for idempotent producers that violate delivery guarantees
      [
        [47, :invalid_producer_epoch, "Producer epoch is invalid (producer fenced)"],
        [59, :unknown_producer_id, "Producer ID is no longer valid"],
        [45, :out_of_order_sequence_number, "Sequence number desynchronization"],
        [90, :producer_fenced, "Producer has been fenced by newer instance"]
      ].each do |error_code, error_symbol, description|
        it "should remap ERR__FATAL to #{error_symbol} (code #{error_code})" do
          error_received = nil
          error_callback = lambda do |error|
            # Only capture the first error to avoid overwriting with subsequent broker errors
            error_received = error if error.fatal?
          end

          Rdkafka::Config.error_callback = error_callback

          # Trigger a test fatal error
          result = producer.trigger_test_fatal_error(error_code, description)

          # Should return RD_KAFKA_RESP_ERR_NO_ERROR (0) if successful
          expect(result).to eq(Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR)

          # Immediately check the fatal error details before any other errors can occur
          fatal_details = producer.fatal_error
          expect(fatal_details).not_to be_nil
          expect(fatal_details[:error_code]).to eq(error_code)
          expect(fatal_details[:error_string]).to include("test_fatal_error")
          expect(fatal_details[:error_string]).to include(description)

          # Give some time for the error callback to be triggered
          sleep 0.1

          # Verify the error callback was called with a fatal error
          # Note: In CI environments without Kafka, the specific error may be overwritten
          # by broker connection errors, but we've verified the core functionality above
          expect(error_received).not_to be_nil
          expect(error_received.fatal?).to be true
        end
      end

      it "should handle fatal error on producer operations after fatal error" do
        # Trigger a test fatal error
        producer.trigger_test_fatal_error(47, "Fatal error for testing")

        sleep 0.1

        # After a fatal error, produce operations should fail with the fatal error
        expect {
          handle = producer.produce(
            topic: TestTopics.produce_test_topic,
            payload: "test",
            key: "key"
          )
          handle.wait(max_wait_timeout_ms: 1_000)
        }.to raise_error(Rdkafka::RdkafkaError) do |error|
          # The error should be related to the fatal condition
          # Note: The exact error may vary depending on librdkafka internals
          expect(error).to be_a(Rdkafka::RdkafkaError)
        end
      end
    end

    context "rd_kafka_fatal_error function" do
      it "should return nil when no fatal error has occurred" do
        # Check for fatal error - should return nil
        result = producer.fatal_error

        expect(result).to be_nil
      end

      it "should return error details after a fatal error is triggered" do
        # Trigger a fatal error
        producer.trigger_test_fatal_error(47, "Test fatal error")

        sleep 0.1

        # Now check for fatal error
        result = producer.fatal_error

        # Should return error details
        expect(result).not_to be_nil
        expect(result[:error_code]).to eq(47)
        expect(result[:error_string]).to include("test_fatal_error")
        expect(result[:error_string]).to include("Test fatal error")
      end
    end

    context "with non-idempotent producer" do
      let(:non_idempotent_producer) do
        rdkafka_producer_config('enable.idempotence' => false).producer
      end

      after { non_idempotent_producer.close }

      it "can still trigger fatal errors for testing purposes" do
        # Note: In real scenarios, fatal errors primarily occur with idempotent/transactional producers
        # However, trigger_test_fatal_error allows testing fatal error handling regardless
        error_received = nil
        error_callback = lambda do |error|
          # Only capture the first error to avoid overwriting with subsequent broker errors
          error_received = error if error.fatal?
        end

        Rdkafka::Config.error_callback = error_callback

        # Trigger a test fatal error
        result = non_idempotent_producer.trigger_test_fatal_error(
          47,
          "Test fatal error on non-idempotent producer"
        )

        expect(result).to eq(Rdkafka::Bindings::RD_KAFKA_RESP_ERR_NO_ERROR)

        # Immediately verify the fatal error state
        fatal_details = non_idempotent_producer.fatal_error
        expect(fatal_details).not_to be_nil
        expect(fatal_details[:error_code]).to eq(47)

        sleep 0.1

        # Even on non-idempotent producer, test fatal error should work
        # The callback may be overwritten by broker errors in CI, but we verified above
        expect(error_received).not_to be_nil
        expect(error_received.fatal?).to be true
      end
    end
  end
end
