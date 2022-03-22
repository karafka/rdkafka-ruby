require "spec_helper"

describe Rdkafka::Producer::Client do
  let(:thread)        { instance_double('Thread') }
  let(:closing)       { false }
  let(:config)        { rdkafka_consumer_config }
  let(:native_config) { config.send(:native_config) }
  let(:native_kafka)  { config.send(:native_kafka, native_config, :rd_kafka_consumer) }

  subject(:client) { described_class.new(native_kafka) }

  before do
    allow(Rdkafka::Bindings).to receive(:rd_kafka_poll).with(instance_of(FFI::Pointer), 250)
    allow(Rdkafka::Bindings).to receive(:rd_kafka_outq_len).with(instance_of(FFI::Pointer)).and_return(0)
    allow(Rdkafka::Bindings).to receive(:rd_kafka_destroy)
    allow(Thread).to receive(:new).and_return(thread)

    allow(thread).to receive(:[]=).with(:closing, anything)
    allow(thread).to receive(:join)
    allow(thread).to receive(:abort_on_exception=).with(anything)
  end

  context "defaults" do
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

    it "polls the native with default 250ms timeout" do
      polling_loop_expects do
        expect(Rdkafka::Bindings).to receive(:rd_kafka_poll).with(native_kafka, 250)
      end
    end

    it "check the out queue of native client" do
      polling_loop_expects do
        expect(Rdkafka::Bindings).to receive(:rd_kafka_outq_len).with(native_kafka)
      end
    end
  end

  def polling_loop_expects(&block)
    Thread.current[:closing] = true # this forces the loop break with line #12

    allow(Thread).to receive(:new).and_yield do |_|
      block.call
    end.and_return(thread)

    client
  end

  it "exposes `native` client" do
    expect(client.native).to eq(native_kafka)
  end

  context "when client was not yet closed (`nil`)" do
    it "is not closed" do
      expect(client.closed?).to eq(false)
    end

    context "and attempt to close" do
      it "calls the `destroy` binding" do
        expect(Rdkafka::Bindings).to receive(:rd_kafka_destroy).with(native_kafka)

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

        expect(client.native).to be_nil
        expect(client.closed?).to eq(true)
      end
    end
  end

  context "when client was already closed" do
    before { client.close }

    it "is closed" do
      expect(client.closed?).to eq(true)
    end

    context "and attempt to close again" do
      it "does not call the `destroy` binding" do
        expect(Rdkafka::Bindings).not_to receive(:rd_kafka_destroy)

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

        expect(client.native).to be_nil
        expect(client.closed?).to eq(true)
      end
    end
  end

  it "provide a finalizer Proc that closes the `native` client" do
    expect(client.closed?).to eq(false)

    client.finalizer.call("some-ignored-object-id")

    expect(client.closed?).to eq(true)
  end
end
