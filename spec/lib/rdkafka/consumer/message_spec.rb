# frozen_string_literal: true

RSpec.describe Rdkafka::Consumer::Message do
  def native_client
    @native_client ||= new_native_client
  end

  def native_topic
    @native_topic ||= new_native_topic(native_client: native_client)
  end

  def build_native_message(payload: nil, key: nil)
    Rdkafka::Bindings::Message.new.tap do |message|
      message[:rkt] = native_topic
      message[:partition] = 3
      message[:offset] = 100
      if payload
        ptr = FFI::MemoryPointer.new(:char, payload.bytesize)
        ptr.put_bytes(0, payload)
        message[:payload] = ptr
        message[:len] = payload.bytesize
      end
      if key
        ptr = FFI::MemoryPointer.new(:char, key.bytesize)
        ptr.put_bytes(0, key)
        message[:key] = ptr
        message[:key_len] = key.bytesize
      end
    end
  end

  def build_message(payload: nil, key: nil)
    Rdkafka::Consumer::Message.new(build_native_message(payload: payload, key: key))
  end

  after do
    Rdkafka::Bindings.rd_kafka_destroy(native_client)
  end

  before do
    # Safety mock: prevents segfault when reading headers from a manually constructed message.
    # See MockBindingsHelpers#stub_message_headers_unavailable for details.
    stub_message_headers_unavailable
  end

  it "has a topic" do
    expect(build_message.topic).to eq "topic_name"
  end

  it "has a partition" do
    expect(build_message.partition).to eq 3
  end

  context "payload" do
    it "has a nil payload when none is present" do
      expect(build_message.payload).to be_nil
    end

    context "present payload" do
      it "has a payload" do
        expect(build_message(payload: "payload content").payload).to eq "payload content"
      end
    end
  end

  context "key" do
    it "has a nil key when none is present" do
      expect(build_message.key).to be_nil
    end

    context "present key" do
      it "has a key" do
        expect(build_message(key: "key content").key).to eq "key content"
      end
    end
  end

  it "has an offset" do
    expect(build_message.offset).to eq 100
  end

  describe "#timestamp" do
    context "without a timestamp" do
      before do
        stub_message_timestamp(-1)
      end

      it "has a nil timestamp if not present" do
        expect(build_message.timestamp).to be_nil
      end
    end

    context "with a timestamp" do
      before do
        stub_message_timestamp(1505069646250)
      end

      it "has timestamp if present" do
        expect(build_message.timestamp).to eq Time.at(1505069646, 250_000)
      end
    end
  end

  describe "#to_s" do
    before do
      stub_message_timestamp(1000000)
    end

    it "has a human readable representation" do
      msg = build_message
      allow(msg).to receive(:timestamp).and_return(1000)
      expect(msg.to_s).to eq "<Message in 'topic_name' with key '', payload '', partition 3, offset 100, timestamp 1000>"
    end

    context "with key and payload" do
      it "has a human readable representation" do
        msg = build_message(key: "key", payload: "payload")
        allow(msg).to receive(:timestamp).and_return(1000)
        expect(msg.to_s).to eq "<Message in 'topic_name' with key 'key', payload 'payload', partition 3, offset 100, timestamp 1000>"
      end
    end

    context "with a very long key and payload" do
      it "has a human readable representation" do
        msg = build_message(key: "k" * 100_000, payload: "p" * 100_000)
        allow(msg).to receive(:timestamp).and_return(1000)
        expect(msg.to_s).to eq "<Message in 'topic_name' with key 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk...', payload 'pppppppppppppppppppppppppppppppppppppppp...', partition 3, offset 100, timestamp 1000>"
      end
    end
  end
end
