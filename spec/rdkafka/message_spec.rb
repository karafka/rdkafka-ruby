require "spec_helper"

describe Rdkafka::Message do
  let(:native_topic) do
    Rdkafka::FFI.rd_kafka_topic_new(
      native_client,
      "topic_name",
      nil
    )
  end
  let(:payload) { nil }
  let(:key) { nil }
  let(:native_message) do
    Rdkafka::FFI::Message.new.tap do |message|
      message[:rkt] = native_topic
      message[:partition] = 3
      message[:offset] = 100
      if payload
        ptr = ::FFI::MemoryPointer.new(:char, payload.bytesize)
        ptr.put_bytes(0, payload)
        message[:payload] = ptr
        message[:len] = payload.bytesize
      end
      if key
        ptr = ::FFI::MemoryPointer.new(:char, key.bytesize)
        ptr.put_bytes(0, key)
        message[:key] = ptr
        message[:key_len] = key.bytesize
      end
    end
  end
  subject { Rdkafka::Message.new(native_message) }

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

  it "should have a timestamp" do
    expect(subject.timestamp).to be > 0
  end
end
