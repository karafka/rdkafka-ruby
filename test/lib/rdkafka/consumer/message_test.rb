# frozen_string_literal: true

describe Rdkafka::Consumer::Message do
  before do
    @native_client = new_native_client
    @native_topic = new_native_topic(native_client: @native_client)
    @payload = nil
    @key = nil

    # mock headers, because it produces 'segmentation fault' while settings or reading headers for
    # a message which is created from scratch
    #
    # Code dump example:
    #
    # ```
    # frame #7: 0x000000010dacf5ab librdkafka.dylib`rd_list_destroy + 11
    # frame #8: 0x000000010dae5a7e librdkafka.dylib`rd_kafka_headers_destroy + 14
    # frame #9: 0x000000010da9ab40 librdkafka.dylib`rd_kafka_message_set_headers + 32
    # ```
    Rdkafka::Bindings
      .expects(:rd_kafka_message_headers)
      .returns(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__NOENT)
  end

  after do
    Rdkafka::Bindings.rd_kafka_destroy(@native_client)
  end

  def build_native_message
    Rdkafka::Bindings::Message.new.tap do |message|
      message[:rkt] = @native_topic
      message[:partition] = 3
      message[:offset] = 100
      if @payload
        ptr = FFI::MemoryPointer.new(:char, @payload.bytesize)
        ptr.put_bytes(0, @payload)
        message[:payload] = ptr
        message[:len] = @payload.bytesize
      end
      if @key
        ptr = FFI::MemoryPointer.new(:char, @key.bytesize)
        ptr.put_bytes(0, @key)
        message[:key] = ptr
        message[:key_len] = @key.bytesize
      end
    end
  end

  def build_message
    described_class.new(build_native_message)
  end

  it "has a topic" do
    assert_equal("topic_name", build_message.topic)
  end

  it "has a partition" do
    assert_equal(3, build_message.partition)
  end

  context "payload" do
    it "has a nil payload when none is present" do
      assert_nil(build_message.payload)
    end

    context "present payload" do
      before { @payload = "payload content" }

      it "has a payload" do
        assert_equal("payload content", build_message.payload)
      end
    end
  end

  context "key" do
    it "has a nil key when none is present" do
      assert_nil(build_message.key)
    end

    context "present key" do
      before { @key = "key content" }

      it "has a key" do
        assert_equal("key content", build_message.key)
      end
    end
  end

  it "has an offset" do
    assert_equal(100, build_message.offset)
  end

  describe "#timestamp" do
    context "without a timestamp" do
      before do
        Rdkafka::Bindings.stubs(:rd_kafka_message_timestamp).returns(-1)
      end

      it "has a nil timestamp if not present" do
        assert_nil(build_message.timestamp)
      end
    end

    context "with a timestamp" do
      before do
        Rdkafka::Bindings.stubs(:rd_kafka_message_timestamp).returns(1505069646250)
      end

      it "has timestamp if present" do
        assert_equal(Time.at(1505069646, 250_000), build_message.timestamp)
      end
    end
  end

  describe "#to_s" do
    def build_message
      super.tap { |message| message.stubs(:timestamp).returns(1000) }
    end

    it "has a human readable representation" do
      assert_equal(
        "<Message in 'topic_name' with key '', payload '', partition 3, offset 100, timestamp 1000>",
        build_message.to_s
      )
    end

    context "with key and payload" do
      before do
        @key = "key"
        @payload = "payload"
      end

      it "has a human readable representation" do
        assert_equal(
          "<Message in 'topic_name' with key 'key', payload 'payload', partition 3, offset 100, timestamp 1000>",
          build_message.to_s
        )
      end
    end

    context "with a very long key and payload" do
      before do
        @key = "k" * 100_000
        @payload = "p" * 100_000
      end

      it "has a human readable representation" do
        assert_equal(
          "<Message in 'topic_name' with key 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk...', payload 'pppppppppppppppppppppppppppppppppppppppp...', partition 3, offset 100, timestamp 1000>",
          build_message.to_s
        )
      end
    end
  end
end
