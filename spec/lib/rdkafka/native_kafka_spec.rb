# frozen_string_literal: true

RSpec.describe Rdkafka::NativeKafka do
  def config
    @config ||= rdkafka_producer_config
  end

  def native
    @native ||= config.send(:native_kafka, config.send(:native_config), :rd_kafka_producer)
  end

  def opaque
    @opaque ||= Rdkafka::Opaque.new
  end

  def thread
    @thread ||= double(Thread)
  end

  def build_client
    Rdkafka::NativeKafka.new(native, run_polling_thread: true, opaque: opaque)
  end

  before do
    stub_binding_return(:rd_kafka_name, "producer-1")
    allow(Thread).to receive(:new).and_return(thread)
    allow(thread).to receive(:name=).with("rdkafka.native_kafka#producer-1")
    allow(thread).to receive(:[]=).with(:closing, anything)
    allow(thread).to receive(:join)
    allow(thread).to receive(:abort_on_exception=).with(anything)
  end

  after do
    @client&.close
  end

  context "defaults" do
    it "sets the thread name" do
      expect(thread).to receive(:name=).with("rdkafka.native_kafka#producer-1")

      @client = build_client
    end

    it "sets the thread to abort on exception" do
      expect(thread).to receive(:abort_on_exception=).with(true)

      @client = build_client
    end

    it "sets the thread `closing` flag to false" do
      expect(thread).to receive(:[]=).with(:closing, false)

      @client = build_client
    end
  end

  context "the polling thread" do
    it "is created" do
      expect(Thread).to receive(:new)

      @client = build_client
    end
  end

  it "exposes the inner client" do
    @client = build_client
    @client.with_inner do |inner|
      expect(inner).to eq(native)
    end
  end

  context "when client was not yet closed (`nil`)" do
    it "is not closed" do
      @client = build_client
      expect(@client.closed?).to be(false)
    end

    context "and attempt to close" do
      it "calls the `destroy` binding" do
        @client = build_client
        expect(Rdkafka::Bindings).to receive(:rd_kafka_destroy).with(native).and_call_original

        @client.close
      end

      it "indicates to the polling thread that it is closing" do
        @client = build_client
        expect(thread).to receive(:[]=).with(:closing, true)

        @client.close
      end

      it "joins the polling thread" do
        @client = build_client
        expect(thread).to receive(:join)

        @client.close
      end

      it "closes and unassign the native client" do
        @client = build_client
        @client.close

        expect(@client.closed?).to be(true)
      end
    end
  end

  context "when client was already closed" do
    before do
      @client = build_client
      @client.close
    end

    it "is closed" do
      expect(@client.closed?).to be(true)
    end

    context "and attempt to close again" do
      it "does not call the `destroy` binding" do
        expect(Rdkafka::Bindings).not_to receive(:rd_kafka_destroy_flags)

        @client.close
      end

      it "does not indicate to the polling thread that it is closing" do
        expect(thread).not_to receive(:[]=).with(:closing, true)

        @client.close
      end

      it "does not join the polling thread" do
        expect(thread).not_to receive(:join)

        @client.close
      end

      it "does not close and unassign the native client again" do
        @client.close

        expect(@client.closed?).to be(true)
      end
    end
  end

  it "provides a finalizer that closes the native kafka client" do
    @client = build_client
    expect(@client.closed?).to be(false)

    @client.finalizer.call("some-ignored-object-id")

    expect(@client.closed?).to be(true)
  end
end

# Separate describe block for FD API tests to avoid interference with mocked threading tests
RSpec.describe Rdkafka::NativeKafka, "#enable_main_queue_io_events and #enable_background_queue_io_events" do
  def config
    @config ||= rdkafka_producer_config
  end

  def native
    @native ||= config.send(:native_kafka, config.send(:native_config), :rd_kafka_producer)
  end

  def opaque
    @opaque ||= Rdkafka::Opaque.new
  end

  def client
    @client ||= Rdkafka::NativeKafka.new(native, run_polling_thread: false, opaque: opaque, auto_start: false)
  end

  after { client.close unless client.closed? }

  it "allows IO events when polling thread is not active" do
    signal_r, signal_w = IO.pipe

    expect { client.enable_main_queue_io_events(signal_w.fileno) }.not_to raise_error
    expect { client.enable_background_queue_io_events(signal_w.fileno) }.not_to raise_error

    signal_r.close
    signal_w.close
  end

  it "accepts custom payload for IO events" do
    signal_r, signal_w = IO.pipe
    payload = "custom"

    expect { client.enable_main_queue_io_events(signal_w.fileno, payload) }.not_to raise_error

    signal_r.close
    signal_w.close
  end

  context "when client is closed" do
    before { client.close }

    it "raises ClosedInnerError when enabling main_queue_io_events" do
      signal_r, signal_w = IO.pipe
      expect { client.enable_main_queue_io_events(signal_w.fileno) }.to raise_error(Rdkafka::ClosedInnerError)
      signal_r.close
      signal_w.close
    end

    it "raises ClosedInnerError when enabling background_queue_io_events" do
      signal_r, signal_w = IO.pipe
      expect { client.enable_background_queue_io_events(signal_w.fileno) }.to raise_error(Rdkafka::ClosedInnerError)
      signal_r.close
      signal_w.close
    end
  end
end
