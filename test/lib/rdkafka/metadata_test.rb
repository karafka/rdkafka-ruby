# frozen_string_literal: true

require "securerandom"

describe Rdkafka::Metadata do
  before do
    config = rdkafka_consumer_config
    native_config = config.send(:native_config)
    @native_kafka = config.send(:native_kafka, native_config, :rd_kafka_consumer)
  end

  after do
    Rdkafka::Bindings.rd_kafka_consumer_close(@native_kafka)
    Rdkafka::Bindings.rd_kafka_destroy(@native_kafka)
  end

  context "passing in a topic name" do
    context "that is non-existent topic" do
      it "raises an appropriate exception" do
        topic_name = TestTopics.non_existing

        error = assert_raises(Rdkafka::RdkafkaError) do
          described_class.new(@native_kafka, topic_name)
        end

        assert_equal("Broker: Unknown topic or partition (unknown_topic_or_part)", error.message)
      end
    end

    context "that is longer than librdkafka accepts" do
      it "raises an exception instead of crashing" do
        error = assert_raises(Rdkafka::RdkafkaError) do
          described_class.new(@native_kafka, "a" * 513)
        end

        assert_equal(:topic_exception, error.code)
      end

      it "does not destroy the missing native topic" do
        Rdkafka::Bindings.expects(:rd_kafka_topic_destroy).never

        assert_raises(Rdkafka::RdkafkaError) { described_class.new(@native_kafka, "a" * 513) }
      end
    end

    context "that is at the librdkafka length limit" do
      it "raises the broker error" do
        error = assert_raises(Rdkafka::RdkafkaError) do
          described_class.new(@native_kafka, "a" * 512)
        end

        assert_equal(:topic_exception, error.code)
      end
    end

    context "that is one of our test topics" do
      before do
        @topic_name = TestTopics.create(partitions: 25)
        @metadata = described_class.new(@native_kafka, @topic_name)
      end

      it "#brokers returns our single broker" do
        assert_equal(1, @metadata.brokers.length)
        assert_equal(1, @metadata.brokers[0][:broker_id])
        assert_includes(%w[127.0.0.1 localhost], @metadata.brokers[0][:broker_name])
        assert_equal(rdkafka_base_config[:"bootstrap.servers"].split(":").last.to_i, @metadata.brokers[0][:broker_port])
      end

      it "#topics returns data on our test topic" do
        assert_equal(1, @metadata.topics.length)
        assert_equal(25, @metadata.topics[0][:partition_count])
        assert_equal(25, @metadata.topics[0][:partitions].length)
        assert_equal(@topic_name, @metadata.topics[0][:topic_name])
      end

      it "#topics exposes the replica and in-sync replica broker ids per partition" do
        @metadata.topics[0][:partitions].each do |partition|
          assert_equal([1], partition[:replicas])
          assert_equal([1], partition[:isrs])
          assert_equal(1, partition[:replica_count])
        end
      end
    end
  end

  context "not passing in a topic name" do
    it "#brokers returns our single broker" do
      metadata = described_class.new(@native_kafka, nil)

      assert_equal(1, metadata.brokers.length)
      assert_equal(1, metadata.brokers[0][:broker_id])
      assert_includes(%w[127.0.0.1 localhost], metadata.brokers[0][:broker_name])
      assert_equal(rdkafka_base_config[:"bootstrap.servers"].split(":").last.to_i, metadata.brokers[0][:broker_port])
    end

    it "#topics returns data about existing topics" do
      # Force topic creation before querying metadata
      test_topic = TestTopics.create
      metadata = described_class.new(@native_kafka, nil)
      result = metadata.topics.map { |topic| topic[:topic_name] }

      assert_includes(result, test_topic)
    end
  end

  context "when a non-zero error code is returned" do
    before do
      Rdkafka::Bindings.stubs(:rd_kafka_metadata).returns(error_code(:unsupported_feature))
    end

    it "creating the instance raises an exception" do
      error = assert_raises(Rdkafka::RdkafkaError) do
        described_class.new(@native_kafka, TestTopics.unique)
      end

      assert_match(/Local: Required feature not supported by broker \(unsupported_feature\)/, error.message)
    end
  end

  context "when the fetch is retried" do
    # Builds a minimal native metadata struct carrying a single topic with the given response
    # error, so the real parse path raises that error after rd_kafka_metadata "allocated" it.
    # Returns the backing pointers (kept referenced so they are not garbage collected).
    def build_metadata_with_topic_error(resp_err)
      topic_buf = FFI::MemoryPointer.new(Rdkafka::Metadata::TopicMetadata.size)
      topic = Rdkafka::Metadata::TopicMetadata.new(topic_buf)
      topic[:partition_count] = 0
      topic[:rd_kafka_resp_err] = resp_err

      meta_buf = FFI::MemoryPointer.new(Rdkafka::Metadata::Metadata.size)
      meta = Rdkafka::Metadata::Metadata.new(meta_buf)
      meta[:brokers_count] = 0
      meta[:topics_count] = 1
      meta[:topics_metadata] = topic_buf

      [meta_buf, topic_buf]
    end

    # Warnings are silenced only around the reassignment, which Ruby reports as a redefinition.
    def stub_default(name, value)
      @original_defaults[name] = Rdkafka::Defaults.const_get(name) unless @original_defaults.key?(name)
      set_default(name, value)
    end

    def set_default(name, value)
      verbose = $VERBOSE
      $VERBOSE = nil
      Rdkafka::Defaults.const_set(name, value)
    ensure
      $VERBOSE = verbose
    end

    before do
      @topic_name = TestTopics.unique
      @original_defaults = {}
      @topic_destroys = 0
      @metadata_destroys = 0

      # Make the backoff zero so sleep is instant between retries.
      stub_default(:METADATA_RETRY_BACKOFF_BASE_MS, 0)
      Rdkafka::Bindings.stubs(:rd_kafka_topic_new).returns(FFI::MemoryPointer.new(:int))
      Rdkafka::Bindings.stubs(:rd_kafka_topic_destroy).with { @topic_destroys += 1 }
      Rdkafka::Bindings.stubs(:rd_kafka_metadata_destroy).with { @metadata_destroys += 1 }
    end

    after do
      @original_defaults.each { |name, value| set_default(name, value) }
    end

    it "destroys every attempt's native topic handle and metadata struct, not just the last" do
      # leader_not_available is retried and surfaces from the parse step, which only runs after
      # rd_kafka_metadata has already allocated the struct.
      err_meta, _err_topic = build_metadata_with_topic_error(error_code(:leader_not_available))
      ok_meta = FFI::MemoryPointer.new(Rdkafka::Metadata::Metadata.size) # zeroed => empty success

      # First two attempts return the erroring struct, the third succeeds.
      structs = [err_meta, err_meta, ok_meta]
      fetch = 0
      Rdkafka::Bindings.stubs(:rd_kafka_metadata).with do |_client, _flag, _topic, ptr, _timeout|
        ptr.write_pointer(structs[fetch])
        fetch += 1
      end.returns(0)

      described_class.new(@native_kafka, @topic_name, 10)

      assert_equal(3, fetch)
      assert_equal(3, @metadata_destroys)
      assert_equal(3, @topic_destroys)
    end

    it "does not destroy a metadata struct when the fetch itself fails before allocating one" do
      # timed_out is retried, but the fetch never succeeds so no struct is allocated.
      Rdkafka::Bindings.stubs(:rd_kafka_metadata).returns(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__TIMED_OUT)

      error = assert_raises(Rdkafka::RdkafkaError) do
        described_class.new(@native_kafka, @topic_name, 10)
      end

      assert_equal(:timed_out, error.code)
      assert_equal(0, @metadata_destroys)
    end

    it "keeps the request timeout constant across retries instead of clobbering it with the backoff" do
      timeouts = []
      Rdkafka::Bindings.stubs(:rd_kafka_metadata).with do |_client, _flag, _topic, _ptr, timeout|
        timeouts << timeout
      end.returns(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__TIMED_OUT) # timed_out => retried until MAX_RETRIES

      assert_raises(Rdkafka::RdkafkaError) { described_class.new(@native_kafka, @topic_name, 2_000) }

      # Every attempt uses the configured request timeout. Before the fix the timeout was
      # overwritten with the backoff (which, with the zeroed base above, would be 0 after attempt 1).
      assert_equal(Rdkafka::Defaults::METADATA_MAX_RETRIES + 1, timeouts.size)
      assert_equal(Array.new(timeouts.size, 2_000), timeouts)
    end

    it "caps the retry backoff so a long retry sequence cannot block for minutes" do
      stub_default(:METADATA_RETRY_BACKOFF_BASE_MS, 100)
      stub_default(:METADATA_RETRY_BACKOFF_MAX_MS, 30)
      stub_default(:METADATA_MAX_RETRIES, 5)
      Rdkafka::Bindings.stubs(:rd_kafka_metadata).returns(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__TIMED_OUT)

      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      assert_raises(Rdkafka::RdkafkaError) { described_class.new(@native_kafka, @topic_name, 10) }
      elapsed_ms = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - started) * 1_000

      # 5 retries capped at 30ms => ~150ms. Uncapped (100 * 2**attempt) would be 200+400+800+1600+
      # 3200 = 6.2s, so a 1s ceiling cleanly separates capped from uncapped.
      assert_operator(elapsed_ms, :<, 1_000)
    end

    it "stops after the minimum attempts once the retry budget is exhausted" do
      stub_default(:METADATA_RETRY_BUDGET_MS, 0)
      calls = 0
      Rdkafka::Bindings.stubs(:rd_kafka_metadata).with { calls += 1 }.returns(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__TIMED_OUT) # timed_out => retryable, never succeeds

      assert_raises(Rdkafka::RdkafkaError) { described_class.new(@native_kafka, @topic_name, 10) }

      # With a zero budget the deadline is immediately past, so the loop ends as soon as the
      # minimum-attempts floor is met: exactly METADATA_MIN_ATTEMPTS tries, neither fewer (the floor
      # is honoured) nor up to METADATA_MAX_RETRIES (the budget stops it).
      assert_equal(Rdkafka::Defaults::METADATA_MIN_ATTEMPTS, calls)
    end
  end
end
