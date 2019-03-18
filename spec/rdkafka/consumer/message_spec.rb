require "spec_helper"

describe Rdkafka::Consumer::Message do
  let(:native_topic) { new_native_topic }
  let(:payload) { nil }
  let(:key) { nil }
  let(:native_message) do
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
  subject { Rdkafka::Consumer::Message.new(native_message) }

  it "should have a topic" do
    expect(subject.topic).to eq "topic_name"
  end

  it "should have a partition" do
    expect(subject.partition).to eq 3
  end

  context "payload" do
    it "should have a nil payload when none is present" do
      expect(subject.payload).to be_nil
    end

    context "present payload" do
      let(:payload) { "payload content" }

      it "should have a payload" do
        expect(subject.payload).to eq "payload content"
      end
    end
  end

  context "key" do
    it "should have a nil key when none is present" do
      expect(subject.key).to be_nil
    end

    context "present key" do
      let(:key) { "key content" }

      it "should have a key" do
        expect(subject.key).to eq "key content"
      end
    end
  end

  it "should have an offset" do
    expect(subject.offset).to eq 100
  end

  describe "#timestamp" do
    context "without a timestamp" do
      before do
        allow(Rdkafka::Bindings).to receive(:rd_kafka_message_timestamp).and_return(-1)
      end

      it "should have a nil timestamp if not present" do
        expect(subject.timestamp).to be_nil
      end
    end

    context "with a timestamp" do
      before do
        allow(Rdkafka::Bindings).to receive(:rd_kafka_message_timestamp).and_return(1505069646250)
      end

      it "should have timestamp if present" do
        expect(subject.timestamp).to eq Time.at(1505069646, 250_000)
      end
    end
  end

  describe "#to_s" do
    before do
      allow(subject).to receive(:timestamp).and_return(1000)
    end

    it "should have a human readable representation" do
      expect(subject.to_s).to eq "<Message in 'topic_name' with key '', payload '', partition 3, offset 100, timestamp 1000, headers empty>"
    end

    context "with key and payload" do
      let(:key) { "key" }
      let(:payload) { "payload" }

      it "should have a human readable representation" do
        expect(subject.to_s).to eq "<Message in 'topic_name' with key 'key', payload 'payload', partition 3, offset 100, timestamp 1000, headers empty>"
      end
    end

    context "with a very long key and payload" do
      let(:key) { "k" * 100_000 }
      let(:payload) { "p" * 100_000 }

      it "should have a human readable representation" do
        expect(subject.to_s).to eq "<Message in 'topic_name' with key 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk...', payload 'pppppppppppppppppppppppppppppppppppppppp...', partition 3, offset 100, timestamp 1000, headers empty>"
      end
    end
  end
end
