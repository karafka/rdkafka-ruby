# frozen_string_literal: true

RSpec.describe Rdkafka::NativeKafka do
  subject(:client) { described_class.new(native, run_polling_thread: true, opaque: opaque) }

  let(:config) { rdkafka_producer_config }
  let(:native) { config.send(:native_kafka, config.send(:native_config), :rd_kafka_producer) }
  let(:closing) { false }
  let(:thread) { double(Thread) }
  let(:opaque) { Rdkafka::Opaque.new }

  before do
    allow(Rdkafka::Bindings).to receive(:rd_kafka_name).and_return("producer-1")
    allow(Thread).to receive(:new).and_return(thread)
    allow(thread).to receive(:name=).with("rdkafka.native_kafka#producer-1")
    allow(thread).to receive(:[]=).with(:closing, anything)
    allow(thread).to receive(:join)
    allow(thread).to receive(:abort_on_exception=).with(anything)
  end

  after { client.close if defined?(@__memoized) && @__memoized.key?(:client) }

  context "defaults" do
    it "sets the thread name" do
      expect(thread).to receive(:name=).with("rdkafka.native_kafka#producer-1")

      client
    end

    it "sets the thread to abort on exception" do
      expect(thread).to receive(:abort_on_exception=).with(true)

      client
    end

    it "sets the thread `closing` flag to false" do
      expect(thread).to receive(:[]=).with(:closing, false)

      client
    end
  end

  context "the polling thread" do
    it "is created" do
      expect(Thread).to receive(:new)

      client
    end
  end

  it "exposes the inner client" do
    client.with_inner do |inner|
      expect(inner).to eq(native)
    end
  end

  context "when client was not yet closed (`nil`)" do
    it "is not closed" do
      expect(client.closed?).to be(false)
    end

    context "and attempt to close" do
      it "calls the `destroy` binding" do
        expect(Rdkafka::Bindings).to receive(:rd_kafka_destroy).with(native).and_call_original

        client.close
      end

      it "indicates to the polling thread that it is closing" do
        expect(thread).to receive(:[]=).with(:closing, true)

        client.close
      end

      it "joins the polling thread" do
        expect(thread).to receive(:join)

        client.close
      end

      it "closes and unassign the native client" do
        client.close

        expect(client.closed?).to be(true)
      end
    end
  end

  context "when client was already closed" do
    before { client.close }

    it "is closed" do
      expect(client.closed?).to be(true)
    end

    context "and attempt to close again" do
      it "does not call the `destroy` binding" do
        expect(Rdkafka::Bindings).not_to receive(:rd_kafka_destroy_flags)

        client.close
      end

      it "does not indicate to the polling thread that it is closing" do
        expect(thread).not_to receive(:[]=).with(:closing, true)

        client.close
      end

      it "does not join the polling thread" do
        expect(thread).not_to receive(:join)

        client.close
      end

      it "does not close and unassign the native client again" do
        client.close

        expect(client.closed?).to be(true)
      end
    end
  end

  it "provides a finalizer that closes the native kafka client" do
    expect(client.closed?).to be(false)

    client.finalizer.call("some-ignored-object-id")

    expect(client.closed?).to be(true)
  end

  context "file descriptor access for fiber scheduler integration" do
    # Create separate native handle for FD API tests to avoid interfering with main tests
    let(:fd_config) { rdkafka_producer_config }
    let(:fd_native) { fd_config.send(:native_kafka, fd_config.send(:native_config), :rd_kafka_producer) }
    let(:fd_opaque) { Rdkafka::Opaque.new }
    let(:fd_client) { described_class.new(fd_native, run_polling_thread: false, opaque: fd_opaque, auto_start: false) }

    after { fd_client.close unless fd_client.closed? }
    # Don't call client.close in this context since we're not using it

    it "allows IO events when polling thread is not active" do
      signal_r, signal_w = IO.pipe

      expect { fd_client.enable_main_queue_io_events(signal_w.fileno) }.not_to raise_error
      expect { fd_client.enable_background_queue_io_events(signal_w.fileno) }.not_to raise_error

      signal_r.close
      signal_w.close
    end

    it "accepts custom payload for IO events" do
      signal_r, signal_w = IO.pipe
      payload = "custom"

      expect { fd_client.enable_main_queue_io_events(signal_w.fileno, payload) }.not_to raise_error

      signal_r.close
      signal_w.close
    end

    context "when client is closed" do
      before { fd_client.close }

      it "raises ClosedInnerError when enabling main_queue_io_events" do
        signal_r, signal_w = IO.pipe
        expect { fd_client.enable_main_queue_io_events(signal_w.fileno) }.to raise_error(Rdkafka::ClosedInnerError)
        signal_r.close
        signal_w.close
      end

      it "raises ClosedInnerError when enabling background_queue_io_events" do
        signal_r, signal_w = IO.pipe
        expect { fd_client.enable_background_queue_io_events(signal_w.fileno) }.to raise_error(Rdkafka::ClosedInnerError)
        signal_r.close
        signal_w.close
      end
    end
  end
end
