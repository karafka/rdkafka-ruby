# frozen_string_literal: true

require_relative "../../test_helper"

describe Rdkafka::NativeKafka do
  def setup
    super
    @config = rdkafka_producer_config
    @native = @config.send(:native_kafka, @config.send(:native_config), :rd_kafka_producer)
    @opaque = Rdkafka::Opaque.new
    @thread = mock("thread")
    @thread.stubs(:name=)
    @thread.stubs(:[]=)
    @thread.stubs(:join)
    @thread.stubs(:abort_on_exception=)
    Rdkafka::Bindings.stubs(:rd_kafka_name).returns("producer-1")
    Thread.stubs(:new).returns(@thread)
  end

  def new_client
    described_class.new(@native, run_polling_thread: true, opaque: @opaque)
  end

  def teardown
    @client.close if @client && !@client.closed?
    super
  end

  context "defaults" do
    it "sets the thread name" do
      @thread.expects(:name=).with("rdkafka.native_kafka#producer-1")
      @client = new_client
    end

    it "sets the thread to abort on exception" do
      @thread.expects(:abort_on_exception=).with(true)
      @client = new_client
    end

    it "sets the thread closing flag to false" do
      @thread.expects(:[]=).with(:closing, false)
      @client = new_client
    end
  end

  context "the polling thread" do
    it "is created" do
      Thread.expects(:new).returns(@thread)
      @client = new_client
    end
  end

  it "exposes the inner client" do
    @client = new_client
    @client.with_inner do |inner|
      assert_equal @native, inner
    end
  end

  context "when client was not yet closed (nil)" do
    before do
      @client = new_client
    end

    it "is not closed" do
      assert_equal false, @client.closed?
    end

    context "and attempt to close" do
      it "calls the destroy binding" do
        Rdkafka::Bindings.expects(:rd_kafka_destroy).with(@native)
        @client.close
      end

      it "indicates to the polling thread that it is closing" do
        @thread.expects(:[]=).with(:closing, true)
        @client.close
      end

      it "joins the polling thread" do
        @thread.expects(:join)
        @client.close
      end

      it "closes and unassign the native client" do
        @client.close
        assert_equal true, @client.closed?
      end
    end
  end

  context "when client was already closed" do
    before do
      @client = new_client
      @client.close
    end

    it "is closed" do
      assert_equal true, @client.closed?
    end

    context "and attempt to close again" do
      it "does not call the destroy_flags binding" do
        Rdkafka::Bindings.expects(:rd_kafka_destroy_flags).never
        @client.close
      end

      it "does not indicate to the polling thread that it is closing" do
        @thread.expects(:[]=).with(:closing, true).never
        @client.close
      end

      it "does not join the polling thread" do
        @thread.expects(:join).never
        @client.close
      end

      it "does not close and unassign the native client again" do
        @client.close
        assert_equal true, @client.closed?
      end
    end
  end

  it "provides a finalizer that closes the native kafka client" do
    @client = new_client
    assert_equal false, @client.closed?

    @client.finalizer.call("some-ignored-object-id")

    assert_equal true, @client.closed?
  end
end

# Separate describe block for FD API tests to avoid interference with mocked threading tests
describe Rdkafka::NativeKafka, "#enable_main_queue_io_events and #enable_background_queue_io_events" do
  def setup
    super
    @config = rdkafka_producer_config
    @native = @config.send(:native_kafka, @config.send(:native_config), :rd_kafka_producer)
    @opaque = Rdkafka::Opaque.new
    @client = described_class.new(@native, run_polling_thread: false, opaque: @opaque, auto_start: false)
  end

  def teardown
    @client.close unless @client.closed?
    super
  end

  it "allows IO events when polling thread is not active" do
    signal_r, signal_w = IO.pipe

    @client.enable_main_queue_io_events(signal_w.fileno)
    @client.enable_background_queue_io_events(signal_w.fileno)

    signal_r.close
    signal_w.close
  end

  it "accepts custom payload for IO events" do
    signal_r, signal_w = IO.pipe
    payload = "custom"

    @client.enable_main_queue_io_events(signal_w.fileno, payload)

    signal_r.close
    signal_w.close
  end

  context "when client is closed" do
    before { @client.close }

    it "raises ClosedInnerError when enabling main_queue_io_events" do
      signal_r, signal_w = IO.pipe
      assert_raises(Rdkafka::ClosedInnerError) { @client.enable_main_queue_io_events(signal_w.fileno) }
      signal_r.close
      signal_w.close
    end

    it "raises ClosedInnerError when enabling background_queue_io_events" do
      signal_r, signal_w = IO.pipe
      assert_raises(Rdkafka::ClosedInnerError) { @client.enable_background_queue_io_events(signal_w.fileno) }
      signal_r.close
      signal_w.close
    end
  end
end
