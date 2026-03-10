# frozen_string_literal: true

describe Rdkafka::Consumer::Message do
  let(:native_client) { new_native_client }
  let(:native_topic) { new_native_topic(native_client: native_client) }
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

  after do
    Rdkafka::Bindings.rd_kafka_destroy(native_client)
  end

  def with_message(&block)
    Rdkafka::Bindings.stub(:rd_kafka_message_headers, ->(*_args) { Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT }) do
      msg = Rdkafka::Consumer::Message.new(native_message)
      block.call(msg)
    end
  end

  it "has a topic" do
    with_message do |msg|
      assert_equal "topic_name", msg.topic
    end
  end

  it "has a partition" do
    with_message do |msg|
      assert_equal 3, msg.partition
    end
  end

  describe "payload" do
    it "has a nil payload when none is present" do
      with_message do |msg|
        assert_nil msg.payload
      end
    end

    describe "present payload" do
      let(:payload) { "payload content" }

      it "has a payload" do
        with_message do |msg|
          assert_equal "payload content", msg.payload
        end
      end
    end
  end

  describe "key" do
    it "has a nil key when none is present" do
      with_message do |msg|
        assert_nil msg.key
      end
    end

    describe "present key" do
      let(:key) { "key content" }

      it "has a key" do
        with_message do |msg|
          assert_equal "key content", msg.key
        end
      end
    end
  end

  it "has an offset" do
    with_message do |msg|
      assert_equal 100, msg.offset
    end
  end

  describe "#timestamp" do
    describe "without a timestamp" do
      it "has a nil timestamp if not present" do
        no_headers = ->(*_args) { Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT }
        no_timestamp = ->(*_args) { -1 }
        Rdkafka::Bindings.stub(:rd_kafka_message_headers, no_headers) do
          Rdkafka::Bindings.stub(:rd_kafka_message_timestamp, no_timestamp) do
            msg = Rdkafka::Consumer::Message.new(native_message)

            assert_nil msg.timestamp
          end
        end
      end
    end

    describe "with a timestamp" do
      it "has timestamp if present" do
        no_headers = ->(*_args) { Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT }
        with_timestamp = ->(*_args) { 1505069646250 }
        Rdkafka::Bindings.stub(:rd_kafka_message_headers, no_headers) do
          Rdkafka::Bindings.stub(:rd_kafka_message_timestamp, with_timestamp) do
            msg = Rdkafka::Consumer::Message.new(native_message)

            assert_equal Time.at(1505069646, 250_000), msg.timestamp
          end
        end
      end
    end
  end

  describe "#to_s" do
    it "has a human readable representation" do
      with_message do |msg|
        msg.stub(:timestamp, 1000) do
          assert_equal "<Message in 'topic_name' with key '', payload '', partition 3, offset 100, timestamp 1000>", msg.to_s
        end
      end
    end

    describe "with key and payload" do
      let(:key) { "key" }
      let(:payload) { "payload" }

      it "has a human readable representation" do
        with_message do |msg|
          msg.stub(:timestamp, 1000) do
            assert_equal "<Message in 'topic_name' with key 'key', payload 'payload', partition 3, offset 100, timestamp 1000>", msg.to_s
          end
        end
      end
    end

    describe "with a very long key and payload" do
      let(:key) { "k" * 100_000 }
      let(:payload) { "p" * 100_000 }

      it "has a human readable representation" do
        with_message do |msg|
          msg.stub(:timestamp, 1000) do
            assert_equal "<Message in 'topic_name' with key 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk...', payload 'pppppppppppppppppppppppppppppppppppppppp...', partition 3, offset 100, timestamp 1000>", msg.to_s
          end
        end
      end
    end
  end
end
