# frozen_string_literal: true

require "spec_helper"

describe Rdkafka::NativeKafka do
  let(:config) { rdkafka_producer_config }
  let(:native) { config.send(:native_kafka, config.send(:native_config), :rd_kafka_producer) }
  let(:closing) { false }
  let(:thread) { double(Thread) }

  subject(:client) { described_class.new(native, run_polling_thread: true) }

  before do
    allow(Thread).to receive(:new).and_return(thread)

    allow(thread).to receive(:[]=).with(:closing, anything)
    allow(thread).to receive(:join)
    allow(thread).to receive(:abort_on_exception=).with(anything)
  end

  after { client.close }

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
  end

  it "exposes the inner client" do
    client.with_inner do |inner|
      expect(inner).to eq(native)
    end
  end

  context "when client was not yet closed (`nil`)" do
    it "is not closed" do
      expect(client.closed?).to eq(false)
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

        expect(client.closed?).to eq(true)
      end
    end
  end

  it "provides a finalizer that closes the native kafka client" do
    expect(client.closed?).to eq(false)

    client.finalizer.call("some-ignored-object-id")

    expect(client.closed?).to eq(true)
  end
end
