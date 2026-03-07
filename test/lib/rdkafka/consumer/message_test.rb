# frozen_string_literal: true

require "test_helper"

class MessageTest < Minitest::Test
  def setup
    super
    @native_client = new_native_client
    @native_topic = new_native_topic(native_client: @native_client)
  end

  def teardown
    Rdkafka::Bindings.rd_kafka_destroy(@native_client)
    super
  end

  def build_message(payload: nil, key: nil)
    Rdkafka::Bindings::Message.new.tap do |message|
      message[:rkt] = @native_topic
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

  def with_message(payload: nil, key: nil, &block)
    native_message = build_message(payload: payload, key: key)
    Rdkafka::Bindings.stub(:rd_kafka_message_headers, ->(*_args) { Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT }) do
      subject = Rdkafka::Consumer::Message.new(native_message)
      block.call(subject)
    end
  end

  def test_has_topic
    with_message do |subject|
      assert_equal "topic_name", subject.topic
    end
  end

  def test_has_partition
    with_message do |subject|
      assert_equal 3, subject.partition
    end
  end

  def test_nil_payload_when_none_present
    with_message do |subject|
      assert_nil subject.payload
    end
  end

  def test_has_payload
    with_message(payload: "payload content") do |subject|
      assert_equal "payload content", subject.payload
    end
  end

  def test_nil_key_when_none_present
    with_message do |subject|
      assert_nil subject.key
    end
  end

  def test_has_key
    with_message(key: "key content") do |subject|
      assert_equal "key content", subject.key
    end
  end

  def test_has_offset
    with_message do |subject|
      assert_equal 100, subject.offset
    end
  end

  def test_nil_timestamp_when_not_present
    native_message = build_message
    no_headers = ->(*_args) { Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT }
    no_timestamp = ->(*_args) { -1 }
    Rdkafka::Bindings.stub(:rd_kafka_message_headers, no_headers) do
      Rdkafka::Bindings.stub(:rd_kafka_message_timestamp, no_timestamp) do
        subject = Rdkafka::Consumer::Message.new(native_message)

        assert_nil subject.timestamp
      end
    end
  end

  def test_has_timestamp_when_present
    native_message = build_message
    no_headers = ->(*_args) { Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT }
    with_timestamp = ->(*_args) { 1505069646250 }
    Rdkafka::Bindings.stub(:rd_kafka_message_headers, no_headers) do
      Rdkafka::Bindings.stub(:rd_kafka_message_timestamp, with_timestamp) do
        subject = Rdkafka::Consumer::Message.new(native_message)

        assert_equal Time.at(1505069646, 250_000), subject.timestamp
      end
    end
  end

  def test_to_s
    with_message do |subject|
      subject.stub(:timestamp, 1000) do
        assert_equal "<Message in 'topic_name' with key '', payload '', partition 3, offset 100, timestamp 1000>", subject.to_s
      end
    end
  end

  def test_to_s_with_key_and_payload
    with_message(key: "key", payload: "payload") do |subject|
      subject.stub(:timestamp, 1000) do
        assert_equal "<Message in 'topic_name' with key 'key', payload 'payload', partition 3, offset 100, timestamp 1000>", subject.to_s
      end
    end
  end

  def test_to_s_with_very_long_key_and_payload
    with_message(key: "k" * 100_000, payload: "p" * 100_000) do |subject|
      subject.stub(:timestamp, 1000) do
        assert_equal "<Message in 'topic_name' with key 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk...', payload 'pppppppppppppppppppppppppppppppppppppppp...', partition 3, offset 100, timestamp 1000>", subject.to_s
      end
    end
  end
end
