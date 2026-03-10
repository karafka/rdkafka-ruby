# frozen_string_literal: true

# Simple thread double that records interactions without strict mock expectations.
# This replaces RSpec's `double(Thread)` with `allow(thread).to receive(...)` permissive stubs.
class ThreadDouble
  attr_accessor :name, :closing, :abort_on_exception

  def initialize
    @store = {}
    @joined = false
  end

  def []=(key, val)
    @store[key] = val
  end

  def [](key)
    @store[key]
  end

  def join
    @joined = true
  end

  def joined?
    @joined
  end
end

describe Rdkafka::NativeKafka do
  let(:config) { rdkafka_producer_config }
  let(:native) { config.send(:native_kafka, config.send(:native_config), :rd_kafka_producer) }
  let(:opaque) { Rdkafka::Opaque.new }
  let(:thread) { ThreadDouble.new }

  let(:client) {
    Rdkafka::Bindings.stub(:rd_kafka_name, "producer-1") do
      Thread.stub(:new, ->(*_args, &_blk) { thread }) do
        Rdkafka::NativeKafka.new(native, run_polling_thread: true, opaque: opaque)
      end
    end
  }

  after do
    unless defined?(@_client_closed)
      client.close unless client.closed?
    end
  end

  describe "defaults" do
    it "sets the thread name" do
      client

      assert_equal "rdkafka.native_kafka#producer-1", thread.name
    end

    it "sets the thread to abort on exception" do
      client

      assert thread.abort_on_exception
    end

    it "sets the thread closing flag to false" do
      client

      refute thread[:closing]
    end
  end

  describe "the polling thread" do
    it "is created" do
      thread_created = false
      custom_thread = nil
      Rdkafka::Bindings.stub(:rd_kafka_name, "producer-1") do
        Thread.stub(:new, ->(*_args, &_blk) {
          thread_created = true
          custom_thread = ThreadDouble.new
          custom_thread
        }) do
          @polling_client = Rdkafka::NativeKafka.new(native, run_polling_thread: true, opaque: opaque)
        end
      end

      assert thread_created
      @polling_client.close
      @_client_closed = true
    end
  end

  it "exposes the inner client" do
    client.with_inner do |inner|
      assert_equal native, inner
    end
  end

  describe "when client was not yet closed" do
    it "is not closed" do
      refute_predicate client, :closed?
    end

    describe "and attempt to close" do
      it "closes and unassigns the native client" do
        client.close
        @_client_closed = true

        assert_predicate client, :closed?
      end

      it "indicates to the polling thread that it is closing" do
        client.close
        @_client_closed = true

        assert thread[:closing]
      end

      it "joins the polling thread" do
        client.close
        @_client_closed = true

        assert_predicate thread, :joined?
      end
    end
  end

  describe "when client was already closed" do
    it "is closed" do
      client.close
      @_client_closed = true

      assert_predicate client, :closed?
    end

    it "double close is safe" do
      client.close
      client.close
      @_client_closed = true

      assert_predicate client, :closed?
    end
  end

  it "provides a finalizer that closes the native kafka client" do
    refute_predicate client, :closed?
    client.finalizer.call("some-ignored-object-id")
    @_client_closed = true

    assert_predicate client, :closed?
  end
end

describe Rdkafka::NativeKafka, "#enable_main_queue_io_events and #enable_background_queue_io_events" do
  let(:config) { rdkafka_producer_config }
  let(:native) { config.send(:native_kafka, config.send(:native_config), :rd_kafka_producer) }
  let(:opaque) { Rdkafka::Opaque.new }
  let(:client) { Rdkafka::NativeKafka.new(native, run_polling_thread: false, opaque: opaque, auto_start: false) }

  after { client.close unless client.closed? }

  it "allows IO events when polling thread is not active" do
    signal_r, signal_w = IO.pipe
    client.enable_main_queue_io_events(signal_w.fileno)
    client.enable_background_queue_io_events(signal_w.fileno)
    signal_r.close
    signal_w.close
  end

  it "accepts custom payload for IO events" do
    signal_r, signal_w = IO.pipe
    payload = "custom"
    client.enable_main_queue_io_events(signal_w.fileno, payload)
    signal_r.close
    signal_w.close
  end

  describe "when client is closed" do
    before { client.close }

    it "raises ClosedInnerError when enabling main_queue_io_events" do
      signal_r, signal_w = IO.pipe
      assert_raises(Rdkafka::ClosedInnerError) do
        client.enable_main_queue_io_events(signal_w.fileno)
      end
      signal_r.close
      signal_w.close
    end

    it "raises ClosedInnerError when enabling background_queue_io_events" do
      signal_r, signal_w = IO.pipe
      assert_raises(Rdkafka::ClosedInnerError) do
        client.enable_background_queue_io_events(signal_w.fileno)
      end
      signal_r.close
      signal_w.close
    end
  end
end
