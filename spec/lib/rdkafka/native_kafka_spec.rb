# frozen_string_literal: true

RSpec.describe Rdkafka::NativeKafka do
  let(:client) { described_class.new(native, run_polling_thread: true, opaque: opaque) }

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

  after { client.close }

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
end

# Separate describe block for FD API tests to avoid interference with mocked threading tests
RSpec.describe Rdkafka::NativeKafka, "#enable_main_queue_io_events and #enable_background_queue_io_events" do
  let(:config) { rdkafka_producer_config }
  let(:native) { config.send(:native_kafka, config.send(:native_config), :rd_kafka_producer) }
  let(:opaque) { Rdkafka::Opaque.new }
  let(:client) { described_class.new(native, run_polling_thread: false, opaque: opaque, auto_start: false) }

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

# Separate describe block with real threads, which the mocked threading tests above stub out
RSpec.describe Rdkafka::NativeKafka, "#with_inner from many threads" do
  context "when many threads use a producer" do
    let(:producer) { rdkafka_producer_config.producer }
    let(:threads_count) { 16 }

    after { producer.close unless producer.closed? }

    it "closes once the threads are done" do
      barrier = Queue.new
      stop = false

      threads = Array.new(threads_count) do
        Thread.new do
          barrier.pop
          producer.queue_size until stop
        end
      end

      threads_count.times { barrier << true }
      sleep(2)
      stop = true
      threads.each(&:join)

      closer = Thread.new { producer.close }

      expect(closer.join(5)).not_to be_nil
    end
  end

  context "when operations overlap so that one is always in progress" do
    let(:consumer) { rdkafka_consumer_config.consumer }

    after { consumer.close unless consumer.closed? }

    it "does not start new operations while close waits" do
      threads = Array.new(2) do |i|
        Thread.new do
          sleep(0.025 * i)

          loop { consumer.poll(50) }
        rescue Rdkafka::ClosedConsumerError, Rdkafka::ClosedInnerError
          nil
        end
      end

      sleep(0.2)
      closer = Thread.new { consumer.close }

      expect(closer.join(5)).not_to be_nil
      threads.each(&:join)
    end
  end
end
