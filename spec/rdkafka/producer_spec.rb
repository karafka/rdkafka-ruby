require "spec_helper"

describe Rdkafka::Producer do
  let(:producer) { rdkafka_config.producer }
  let(:consumer) { rdkafka_config.consumer }

  after do
    # Registry should always end up being empty
    expect(Rdkafka::Producer::DeliveryHandle::REGISTRY).to be_empty
    producer.close
    consumer.close
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
          expect(report.partition).to eq 1
          expect(report.offset).to be >= 0
          @callback_called = true
        end

        # Produce a message
        handle = producer.produce(
          topic:   "produce_test_topic",
          payload: "payload",
          key:     "key"
        )

        # Wait for it to be delivered
        handle.wait(max_wait_timeout: 15)

        # Join the producer thread.
        producer.close

        # Callback should have been called
        expect(@callback_called).to be true
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
          topic:   "produce_test_topic",
          payload: "payload",
          key:     "key"
        )

        # Wait for it to be delivered
        handle.wait(max_wait_timeout: 15)

        # Join the producer thread.
        producer.close

        # Callback should have been called
        expect(called_report.first).not_to be_nil
        expect(called_report.first.partition).to eq 1
        expect(called_report.first.offset).to be >= 0
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
      topic:   "produce_test_topic",
      payload: "payload",
      key:     "key"
    )

    # Should be pending at first
    expect(handle.pending?).to be true

    # Check delivery handle and report
    report = handle.wait(max_wait_timeout: 5)
    expect(handle.pending?).to be false
    expect(report).not_to be_nil
    expect(report.partition).to eq 1
    expect(report.offset).to be >= 0

    # Close producer
    producer.close

    # Consume message and verify it's content
    message = wait_for_message(
      topic: "produce_test_topic",
      delivery_report: report,
      consumer: consumer
    )
    expect(message.partition).to eq 1
    expect(message.payload).to eq "payload"
    expect(message.key).to eq "key"
    # Since api.version.request is on by default we will get
    # the message creation timestamp if it's not set.
    expect(message.timestamp).to be_within(10).of(Time.now)
  end

  it "should produce a message with a specified partition" do
    # Produce a message
    handle = producer.produce(
      topic:     "produce_test_topic",
      payload:   "payload partition",
      key:       "key partition",
      partition: 1
    )
    report = handle.wait(max_wait_timeout: 5)

    # Consume message and verify it's content
    message = wait_for_message(
      topic: "produce_test_topic",
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
      partition_count = producer.partition_count('partitioner_test_topic')
      break if (Zlib.crc32(key) % partition_count) != (Zlib.crc32(partition_key) % partition_count)
    end

    # Produce a message with key, partition_key and key + partition_key
    messages = [{key: key}, {partition_key: partition_key}, {key: key, partition_key: partition_key}]

    messages = messages.map do |m|
      handle = producer.produce(
        topic:     "partitioner_test_topic",
        payload:   "payload partition",
        key:       m[:key],
        partition_key: m[:partition_key]
      )
      report = handle.wait(max_wait_timeout: 5)

      wait_for_message(
        topic: "partitioner_test_topic",
        delivery_report: report,
      )
    end

    expect(messages[0].partition).not_to eq(messages[2].partition)
    expect(messages[1].partition).to eq(messages[2].partition)
    expect(messages[0].key).to eq key
    expect(messages[1].key).to be_nil
    expect(messages[2].key).to eq key
  end

  it "should produce a message with utf-8 encoding" do
    handle = producer.produce(
      topic:   "produce_test_topic",
      payload: "Τη γλώσσα μου έδωσαν ελληνική",
      key:     "key utf8"
    )
    report = handle.wait(max_wait_timeout: 5)

    # Consume message and verify it's content
    message = wait_for_message(
      topic: "produce_test_topic",
      delivery_report: report,
      consumer: consumer
    )

    expect(message.partition).to eq 1
    expect(message.payload.force_encoding("utf-8")).to eq "Τη γλώσσα μου έδωσαν ελληνική"
    expect(message.key).to eq "key utf8"
  end

  context "timestamp" do
    it "should raise a type error if not nil, integer or time" do
      expect {
        producer.produce(
          topic:     "produce_test_topic",
          payload:   "payload timestamp",
          key:       "key timestamp",
          timestamp: "10101010"
        )
      }.to raise_error TypeError
    end

    it "should produce a message with an integer timestamp" do
      handle = producer.produce(
        topic:     "produce_test_topic",
        payload:   "payload timestamp",
        key:       "key timestamp",
        timestamp: 1505069646252
      )
      report = handle.wait(max_wait_timeout: 5)

      # Consume message and verify it's content
      message = wait_for_message(
        topic: "produce_test_topic",
        delivery_report: report,
        consumer: consumer
      )

      expect(message.partition).to eq 2
      expect(message.key).to eq "key timestamp"
      expect(message.timestamp).to eq Time.at(1505069646, 252_000)
    end

    it "should produce a message with a time timestamp" do
      handle = producer.produce(
        topic:     "produce_test_topic",
        payload:   "payload timestamp",
        key:       "key timestamp",
        timestamp: Time.at(1505069646, 353_000)
      )
      report = handle.wait(max_wait_timeout: 5)

      # Consume message and verify it's content
      message = wait_for_message(
        topic: "produce_test_topic",
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
      topic:   "produce_test_topic",
      payload: "payload no key"
    )
    report = handle.wait(max_wait_timeout: 5)

    # Consume message and verify it's content
    message = wait_for_message(
      topic: "produce_test_topic",
      delivery_report: report,
      consumer: consumer
    )

    expect(message.key).to be_nil
    expect(message.payload).to eq "payload no key"
  end

  it "should produce a message with nil payload" do
    handle = producer.produce(
      topic: "produce_test_topic",
      key:   "key no payload"
    )
    report = handle.wait(max_wait_timeout: 5)

    # Consume message and verify it's content
    message = wait_for_message(
      topic: "produce_test_topic",
      delivery_report: report,
      consumer: consumer
    )

    expect(message.key).to eq "key no payload"
    expect(message.payload).to be_nil
  end

  it "should produce a message with headers" do
    handle = producer.produce(
      topic:     "produce_test_topic",
      payload:   "payload headers",
      key:       "key headers",
      headers:   { foo: :bar, baz: :foobar }
    )
    report = handle.wait(max_wait_timeout: 5)

    # Consume message and verify it's content
    message = wait_for_message(
      topic: "produce_test_topic",
      delivery_report: report,
      consumer: consumer
    )

    expect(message.payload).to eq "payload headers"
    expect(message.key).to eq "key headers"
    expect(message.headers[:foo]).to eq "bar"
    expect(message.headers[:baz]).to eq "foobar"
    expect(message.headers[:foobar]).to be_nil
  end

  it "should produce a message with empty headers" do
    handle = producer.produce(
      topic:     "produce_test_topic",
      payload:   "payload headers",
      key:       "key headers",
      headers:   {}
    )
    report = handle.wait(max_wait_timeout: 5)

    # Consume message and verify it's content
    message = wait_for_message(
      topic: "produce_test_topic",
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
          topic:   "produce_test_topic",
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

  it "should produce a message in a forked process" do
    # Fork, produce a message, send the report over a pipe and
    # wait for and check the message in the main process.

    # Kernel#fork is not available in JRuby
    skip if defined?(JRUBY_VERSION)

    reader, writer = IO.pipe

    fork do
      reader.close

      # Avoids sharing the socket between processes.
      producer = rdkafka_config.producer

      handle = producer.produce(
        topic:   "produce_test_topic",
        payload: "payload-forked",
        key:     "key-forked"
      )

      report = handle.wait(max_wait_timeout: 5)

      report_json = JSON.generate(
        "partition" => report.partition,
        "offset" => report.offset
      )

      writer.write(report_json)
      writer.close
      producer.close
    end

    writer.close
    report_hash = JSON.parse(reader.read)
    report = Rdkafka::Producer::DeliveryReport.new(
      report_hash["partition"],
      report_hash["offset"]
    )

    reader.close

    # Consume message and verify it's content
    message = wait_for_message(
      topic: "produce_test_topic",
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
        topic:   "produce_test_topic",
        key:     "key error"
      )
    }.to raise_error Rdkafka::RdkafkaError
  end

  it "should raise a timeout error when waiting too long" do
    handle = producer.produce(
      topic:   "produce_test_topic",
      payload: "payload timeout",
      key:     "key timeout"
    )
    expect {
      handle.wait(max_wait_timeout: 0)
    }.to raise_error Rdkafka::Producer::DeliveryHandle::WaitTimeoutError

    # Waiting a second time should work
    handle.wait(max_wait_timeout: 5)
  end

  context "methods that should not be called after a producer has been closed" do
    before do
      producer.close
    end

    # Affected methods and a non-invalid set of parameters for the method
    {
        :produce         => { topic: nil },
        :partition_count => nil,
    }.each do |method, args|
      it "raises an exception if #{method} is called" do
        expect {
          if args.is_a?(Hash)
            producer.public_send(method, **args)
          else
            producer.public_send(method, args)
          end
        }.to raise_exception(Rdkafka::ClosedProducerError, /#{method.to_s}/)
      end
    end
  end
end
