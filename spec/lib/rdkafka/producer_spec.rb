# frozen_string_literal: true

require "zlib"

describe Rdkafka::Producer do
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
          producer.produce(topic: 'test', payload: '', topic_config: { 'invalid': 'invalid' })
        end.to raise_error(Rdkafka::Config::ConfigError)
      end
    end

    context 'when config is valid' do
      it 'expect to raise error' do
        expect do
          producer.produce(topic: 'test', payload: '', topic_config: { 'acks': 1 }).wait
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
              topic: 'produce_config_test',
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
          expect(report.topic_name).to eq "produce_test_topic"
          @callback_called = true
        end

        # Produce a message
        handle = producer.produce(
          topic:   "produce_test_topic",
          payload: "payload",
          key:     "key",
          label:   "label"
        )

        expect(handle.label).to eq "label"

        # Wait for it to be delivered
        handle.wait(max_wait_timeout: 15)

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
          topic:   "produce_test_topic",
          payload: "payload",
          key:     "key"
        )

        # Wait for it to be delivered
        handle.wait(max_wait_timeout: 15)

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
        expect(called_report.first.topic_name).to eq "produce_test_topic"
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
          topic:   "produce_test_topic",
          payload: "payload",
          key:     "key"
        )

        # Wait for it to be delivered
        handle.wait(max_wait_timeout: 15)

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
      topic:   "produce_test_topic",
      payload: "payload",
      key:     "key",
      label:   "label"
    )

    # Should be pending at first
    expect(handle.pending?).to be true
    expect(handle.label).to eq "label"

    # Check delivery handle and report
    report = handle.wait(max_wait_timeout: 5)
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

    # Consume message and verify its content
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

  it "should produce a message with empty string without crashing" do
    messages = [{key: 'a', partition_key: ''}]

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

    expect(messages[0].partition).to be >= 0
    expect(messages[0].key).to eq 'a'
  end

  it "should produce a message with utf-8 encoding" do
    handle = producer.produce(
      topic:   "produce_test_topic",
      payload: "Τη γλώσσα μου έδωσαν ελληνική",
      key:     "key utf8"
    )
    report = handle.wait(max_wait_timeout: 5)

    # Consume message and verify its content
    message = wait_for_message(
      topic: "produce_test_topic",
      delivery_report: report,
      consumer: consumer
    )

    expect(message.partition).to eq 1
    expect(message.payload.force_encoding("utf-8")).to eq "Τη γλώσσα μου έδωσαν ελληνική"
    expect(message.key).to eq "key utf8"
  end

  it "should produce a message to a non-existing topic with key and partition key" do
    new_topic = "it-#{SecureRandom.uuid}"

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
    report = handle.wait(max_wait_timeout: 5)
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
    # Since api.version.request is on by default we will get
    # the message creation timestamp if it's not set.
    expect(message.timestamp).to be_within(10).of(Time.now)
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

      # Consume message and verify its content
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

      # Consume message and verify its content
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

    # Consume message and verify its content
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

    # Consume message and verify its content
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

    # Consume message and verify its content
    message = wait_for_message(
      topic: "produce_test_topic",
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
      topic:     "produce_test_topic",
      payload:   "payload headers",
      key:       "key headers",
      headers:   {}
    )
    report = handle.wait(max_wait_timeout: 5)

    # Consume message and verify its content
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

  it "should produce a message in a forked process", skip: defined?(JRUBY_VERSION) && "Kernel#fork is not available" do
    # Fork, produce a message, send the report over a pipe and
    # wait for and check the message in the main process.
    reader, writer = IO.pipe

    pid = fork do
      reader.close

      # Avoid sharing the client between processes.
      producer = rdkafka_producer_config.producer

      handle = producer.produce(
        topic:   "produce_test_topic",
        payload: "payload-forked",
        key:     "key-forked"
      )

      report = handle.wait(max_wait_timeout: 5)

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

  context "when not being able to deliver the message" do
    let(:producer) do
      rdkafka_producer_config(
        "bootstrap.servers": "127.0.0.1:9093",
        "message.timeout.ms": 100
      ).producer
    end

    it "should contain the error in the response when not deliverable" do
      handler = producer.produce(topic: 'produce_test_topic', payload: nil, label: 'na')
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
      handler = producer.produce(topic: "it-#{SecureRandom.uuid}", payload: nil, label: 'na')
      # Wait for the async callbacks and delivery registry to update
      sleep(2)
      expect(handler.create_result.error).to be_a(Rdkafka::RdkafkaError)
      expect(handler.create_result.error.code).to eq(:msg_timed_out)
      expect(handler.create_result.label).to eq('na')
    end
  end

  describe '#partition_count' do
    it { expect(producer.partition_count('consume_test_topic')).to eq(3) }

    context 'when the partition count value is already cached' do
      before do
        producer.partition_count('consume_test_topic')
        allow(::Rdkafka::Metadata).to receive(:new).and_call_original
      end

      it 'expect not to query it again' do
        producer.partition_count('consume_test_topic')
        expect(::Rdkafka::Metadata).not_to have_received(:new)
      end
    end

    context 'when the partition count value was cached but time expired' do
      before do
        ::Rdkafka::Producer.partitions_count_cache = Rdkafka::Producer::PartitionsCountCache.new
        allow(::Rdkafka::Metadata).to receive(:new).and_call_original
      end

      it 'expect to query it again' do
        producer.partition_count('consume_test_topic')
        expect(::Rdkafka::Metadata).to have_received(:new)
      end
    end

    context 'when the partition count value was cached and time did not expire' do
      before do
        allow(::Process).to receive(:clock_gettime).and_return(0, 29.001)
        producer.partition_count('consume_test_topic')
        allow(::Rdkafka::Metadata).to receive(:new).and_call_original
      end

      it 'expect not to query it again' do
        producer.partition_count('consume_test_topic')
        expect(::Rdkafka::Metadata).not_to have_received(:new)
      end
    end
  end

  describe '#flush' do
    it "should return flush when it can flush all outstanding messages or when no messages" do
      producer.produce(
        topic:     "produce_test_topic",
        payload:   "payload headers",
        key:       "key headers",
        headers:   {}
      )

      expect(producer.flush(5_000)).to eq(true)
    end

    context 'when it cannot flush due to a timeout' do
      let(:producer) do
        rdkafka_producer_config(
          "bootstrap.servers": "127.0.0.1:9093",
          "message.timeout.ms": 2_000
        ).producer
      end

      after do
        # Allow rdkafka to evict message preventing memory-leak
        # Set to 5s because slow CIs need time
        sleep(5)
      end

      it "should return false on flush when cannot deliver and beyond timeout" do
        producer.produce(
          topic:     "produce_test_topic",
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
          "bootstrap.servers": "127.0.0.1:9093",
          "message.timeout.ms": 2_000
        ).producer
      end

      it "should should purge and move forward" do
        producer.produce(
          topic:     "produce_test_topic",
          payload:   "payload headers"
        )

        expect(producer.purge).to eq(true)
        expect(producer.flush(1_000)).to eq(true)
      end

      it "should materialize the delivery handles" do
        handle = producer.produce(
          topic:     "produce_test_topic",
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
            topic:     "produce_test_topic",
            payload:   "payload headers"
          )

          expect(producer.purge).to eq(true)
          # queue purge
          expect(delivery_reports[0].error).to eq(-152)
        end
      end
    end
  end

  describe '#oauthbearer_set_token' do
    context 'when sasl not configured' do
      it 'should return RD_KAFKA_RESP_ERR__STATE' do
        response = producer.oauthbearer_set_token(
              token: "foo",
              lifetime_ms: Time.now.to_i*1000 + 900 * 1000,
              principal_name: "kafka-cluster"
            )
        expect(response).to eq(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE)
      end
    end

    context 'when sasl configured' do
      it 'should succeed' do
        producer_sasl = rdkafka_producer_config(
          {
            "security.protocol": "sasl_ssl",
            "sasl.mechanisms": 'OAUTHBEARER'
          }
        ).producer
        response = producer_sasl.oauthbearer_set_token(
          token: "foo",
          lifetime_ms: Time.now.to_i*1000 + 900 * 1000,
          principal_name: "kafka-cluster"
        )
        expect(response).to eq(0)
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
        topic:     "consume_test_topic",
        key:       "key headers",
        headers:   headers
      ).wait

      message = wait_for_message(topic: "consume_test_topic", consumer: consumer, delivery_report: report)
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
        topic:     "consume_test_topic",
        key:       "key headers",
        headers:   headers
      ).wait

      message = wait_for_message(topic: "consume_test_topic", consumer: consumer, delivery_report: report)
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
    let(:pre_statistics_ttl) { count_cache_hash.fetch('produce_test_topic', [])[0] }
    let(:post_statistics_ttl) { count_cache_hash.fetch('produce_test_topic', [])[0] }

    context "when using partition key" do
      before do
        Rdkafka::Config.statistics_callback = ->(*) {}

        # This call will make a blocking request to the metadata cache
        producer.produce(
          topic:         "produce_test_topic",
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
          topic:         "produce_test_topic",
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
    let(:pre_statistics_ttl) { count_cache_hash.fetch('produce_test_topic', [])[0] }
    let(:post_statistics_ttl) { count_cache_hash.fetch('produce_test_topic', [])[0] }

    context "when using partition key" do
      before do
        # This call will make a blocking request to the metadata cache
        producer.produce(
          topic:         "produce_test_topic",
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
          topic:         "produce_test_topic",
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
            topic: "partitioner_test_topic",
            payload: "test payload",
            partition_key: test_key,
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout: 5)
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
            topic: "partitioner_test_topic",
            payload: "test payload",
            key: "test-key",
            partition_key: "",
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout: 5)
          expect(report.partition).to be >= 0
        end
      end
    end

    context "nil partition key" do
      it "should handle nil partition key gracefully" do
        handle = producer.produce(
          topic: "partitioner_test_topic",
          payload: "test payload",
          key: "test-key",
          partition_key: nil
        )

        report = handle.wait(max_wait_timeout: 5)
        expect(report.partition).to be >= 0
        expect(report.partition).to be < producer.partition_count("partitioner_test_topic")
      end
    end

    context "various key types and lengths with different partitioners" do
      it "should handle very short keys with all partitioners" do
        all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: "partitioner_test_topic",
            payload: "test payload",
            partition_key: "a",
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout: 5)
          expect(report.partition).to be >= 0
          expect(report.partition).to be < producer.partition_count("partitioner_test_topic")
        end
      end

      it "should handle very long keys with all partitioners" do
        long_key = "a" * 1000

        all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: "partitioner_test_topic",
            payload: "test payload",
            partition_key: long_key,
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout: 5)
          expect(report.partition).to be >= 0
          expect(report.partition).to be < producer.partition_count("partitioner_test_topic")
        end
      end

      it "should handle unicode keys with all partitioners" do
        unicode_key = "测试键值🚀"

        all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: "partitioner_test_topic",
            payload: "test payload",
            partition_key: unicode_key,
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout: 5)
          expect(report.partition).to be >= 0
          expect(report.partition).to be < producer.partition_count("partitioner_test_topic")
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
              topic: "partitioner_test_topic",
              payload: "test payload #{Time.now.to_f}",
              partition_key: partition_key,
              partitioner: partitioner
            )
            handle.wait(max_wait_timeout: 5)
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
              topic: "partitioner_test_topic",
              payload: "test payload #{Time.now.to_f}",
              partition_key: partition_key,
              partitioner: partitioner
            )
            handle.wait(max_wait_timeout: 5)
          end

          partitions = reports.map(&:partition)

          # Just ensure they're valid partitions
          partitions.each do |partition|
            expect(partition).to be >= 0
            expect(partition).to be < producer.partition_count("partitioner_test_topic")
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
              topic: "partitioner_test_topic",
              payload: "test payload",
              partition_key: key,
              partitioner: partitioner
            )
            handle.wait(max_wait_timeout: 5)
          end

          partitions = reports.map(&:partition).uniq

          # Should distribute across multiple partitions for most partitioners
          # (though some might hash all keys to same partition by chance)
          expect(partitions.all? { |p| p >= 0 && p < producer.partition_count("partitioner_test_topic") }).to be true
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
          topic: "partitioner_test_topic",
          payload: "test payload 1",
          key: regular_key,
          partition_key: partition_key
        )

        # Message with only partition key (should go to same partition)
        handle2 = producer.produce(
          topic: "partitioner_test_topic",
          payload: "test payload 2",
          partition_key: partition_key
        )

        # Message with only regular key (should go to different partition)
        handle3 = producer.produce(
          topic: "partitioner_test_topic",
          payload: "test payload 3",
          key: regular_key
        )

        report1 = handle1.wait(max_wait_timeout: 5)
        report2 = handle2.wait(max_wait_timeout: 5)
        report3 = handle3.wait(max_wait_timeout: 5)

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
            topic: "partitioner_test_topic",
            payload: "test payload",
            key: "test-key",
            partition_key: nil,
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout: 5)
          expect(report.partition).to be >= 0
          expect(report.partition).to be < producer.partition_count("partitioner_test_topic")
        end
      end

      it "should handle whitespace-only partition key with all partitioners" do
        all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: "partitioner_test_topic",
            payload: "test payload",
            partition_key: "   ",
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout: 5)
          expect(report.partition).to be >= 0
          expect(report.partition).to be < producer.partition_count("partitioner_test_topic")
        end
      end

      it "should handle newline characters in partition key with all partitioners" do
        all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: "partitioner_test_topic",
            payload: "test payload",
            partition_key: "key\nwith\nnewlines",
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout: 5)
          expect(report.partition).to be >= 0
          expect(report.partition).to be < producer.partition_count("partitioner_test_topic")
        end
      end
    end

    context "debugging partitioner issues" do
      it "should show if all partitioners return 0 (indicating a problem)" do
        test_key = "debug-test-key"
        zero_count = 0

        all_partitioners.each do |partitioner|
          handle = producer.produce(
            topic: "partitioner_test_topic",
            payload: "debug payload",
            partition_key: test_key,
            partitioner: partitioner
          )

          report = handle.wait(max_wait_timeout: 5)
          zero_count += 1 if report.partition == 0
        end

        expect(zero_count).to be < all_partitioners.size
      end
    end
  end
end
