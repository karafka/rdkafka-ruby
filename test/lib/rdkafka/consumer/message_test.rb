# frozen_string_literal: true

require_relative "../../../test_helper"

describe Rdkafka::Consumer::Message do
  before do
    @native_client = new_native_client
    @native_topic = new_native_topic(native_client: @native_client)
    @payload = nil
    @key = nil
  end

  after do
    Rdkafka::Bindings.rd_kafka_destroy(@native_client)
  end

  def build_native_message(payload: @payload, key: @key)
    Rdkafka::Bindings::Message.new.tap do |msg|
      msg[:rkt] = @native_topic
      msg[:partition] = 3
      msg[:offset] = 100
      if payload
        ptr = FFI::MemoryPointer.new(:char, payload.bytesize)
        ptr.put_bytes(0, payload)
        msg[:payload] = ptr
        msg[:len] = payload.bytesize
      end
      if key
        ptr = FFI::MemoryPointer.new(:char, key.bytesize)
        ptr.put_bytes(0, key)
        msg[:key] = ptr
        msg[:key_len] = key.bytesize
      end
    end
  end

  def build_message(payload: @payload, key: @key)
    native_msg = build_native_message(payload: payload, key: key)
    # Mock headers to avoid segfault when reading headers from a message created from scratch
    Rdkafka::Bindings.stubs(:rd_kafka_message_headers).returns(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT)
    described_class.new(native_msg)
  end

  it "has a topic" do
    message = build_message
    assert_equal "topic_name", message.topic
  end

  it "has a partition" do
    message = build_message
    assert_equal 3, message.partition
  end

  context "payload" do
    it "has a nil payload when none is present" do
      message = build_message
      assert_nil message.payload
    end

    context "present payload" do
      it "has a payload" do
        message = build_message(payload: "payload content")
        assert_equal "payload content", message.payload
      end
    end
  end

  context "key" do
    it "has a nil key when none is present" do
      message = build_message
      assert_nil message.key
    end

    context "present key" do
      it "has a key" do
        message = build_message(key: "key content")
        assert_equal "key content", message.key
      end
    end
  end

  it "has an offset" do
    message = build_message
    assert_equal 100, message.offset
  end

  describe "#timestamp" do
    context "without a timestamp" do
      it "has a nil timestamp if not present" do
        Rdkafka::Bindings.stubs(:rd_kafka_message_timestamp).returns(-1)
        message = build_message
        assert_nil message.timestamp
      end
    end

    context "with a timestamp" do
      it "has timestamp if present" do
        Rdkafka::Bindings.stubs(:rd_kafka_message_timestamp).returns(1505069646250)
        message = build_message
        assert_equal Time.at(1505069646, 250_000), message.timestamp
      end
    end
  end

  describe "#to_s" do
    it "has a human readable representation" do
      message = build_message
      message.stubs(:timestamp).returns(1000)
      assert_equal "<Message in 'topic_name' with key '', payload '', partition 3, offset 100, timestamp 1000>", message.to_s
    end

    context "with key and payload" do
      it "has a human readable representation" do
        message = build_message(key: "key", payload: "payload")
        message.stubs(:timestamp).returns(1000)
        assert_equal "<Message in 'topic_name' with key 'key', payload 'payload', partition 3, offset 100, timestamp 1000>", message.to_s
      end
    end

    context "with a very long key and payload" do
      it "has a human readable representation" do
        message = build_message(key: "k" * 100_000, payload: "p" * 100_000)
        message.stubs(:timestamp).returns(1000)
        assert_equal "<Message in 'topic_name' with key 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk...', payload 'pppppppppppppppppppppppppppppppppppppppp...', partition 3, offset 100, timestamp 1000>", message.to_s
      end
    end
  end
end
