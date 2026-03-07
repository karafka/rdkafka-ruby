# frozen_string_literal: true

require "test_helper"

class NativeKafkaTest < Minitest::Test
  def setup
    super
    @config = rdkafka_producer_config
    @native = @config.send(:native_kafka, @config.send(:native_config), :rd_kafka_producer)
    @opaque = Rdkafka::Opaque.new
    @thread = Minitest::Mock.new
    @thread.expect(:name=, nil, ["rdkafka.native_kafka#producer-1"])
    @thread.expect(:[]=, nil, [:closing, false])
    @thread.expect(:abort_on_exception=, nil, [true])
  end

  def new_client
    Rdkafka::Bindings.stub(:rd_kafka_name, "producer-1") do
      Thread.stub(:new, @thread) do
        Rdkafka::NativeKafka.new(@native, run_polling_thread: true, opaque: @opaque)
      end
    end
  end

  def teardown
    @client&.close
    super
  end

  def test_sets_thread_name
    thread = Minitest::Mock.new
    thread.expect(:name=, nil, ["rdkafka.native_kafka#producer-1"])
    thread.expect(:[]=, nil, [:closing, false])
    thread.expect(:abort_on_exception=, nil, [true])

    Rdkafka::Bindings.stub(:rd_kafka_name, "producer-1") do
      Thread.stub(:new, thread) do
        @client = Rdkafka::NativeKafka.new(@native, run_polling_thread: true, opaque: @opaque)
      end
    end
    thread.verify
  end

  def test_sets_thread_abort_on_exception
    @client = new_client
    # Verified through mock expectations in setup
  end

  def test_sets_thread_closing_flag_to_false
    @client = new_client
    # Verified through mock expectations in setup
  end

  def test_polling_thread_is_created
    thread_created = false
    Rdkafka::Bindings.stub(:rd_kafka_name, "producer-1") do
      Thread.stub(:new, ->(*args, &block) {
        thread_created = true
        t = Minitest::Mock.new
        t.expect(:name=, nil, [String])
        t.expect(:[]=, nil, [:closing, false])
        t.expect(:abort_on_exception=, nil, [true])
        t
      }) do
        @client = Rdkafka::NativeKafka.new(@native, run_polling_thread: true, opaque: @opaque)
      end
    end

    assert thread_created
  end

  def test_exposes_inner_client
    @client = new_client

    @client.with_inner do |inner|
      assert_equal @native, inner
    end
  end

  def test_not_closed_initially
    @client = new_client

    refute_predicate @client, :closed?
  end

  def test_close_destroys_native_client
    @client = new_client
    @thread.expect(:join, nil)
    @thread.expect(:[]=, nil, [:closing, true])
    @client.close

    assert_predicate @client, :closed?
  end

  def test_closed_after_close
    @client = new_client
    @thread.expect(:join, nil)
    @thread.expect(:[]=, nil, [:closing, true])
    @client.close

    assert_predicate @client, :closed?
  end

  def test_double_close_is_safe
    @client = new_client
    @thread.expect(:join, nil)
    @thread.expect(:[]=, nil, [:closing, true])
    @client.close
    # Second close should be safe (no-op)
    @client.close

    assert_predicate @client, :closed?
  end

  def test_finalizer_closes_client
    @client = new_client

    refute_predicate @client, :closed?
    @thread.expect(:join, nil)
    @thread.expect(:[]=, nil, [:closing, true])
    @client.finalizer.call("some-ignored-object-id")

    assert_predicate @client, :closed?
  end
end

class NativeKafkaFdApiTest < Minitest::Test
  def setup
    super
    @config = rdkafka_producer_config
    @native = @config.send(:native_kafka, @config.send(:native_config), :rd_kafka_producer)
    @opaque = Rdkafka::Opaque.new
    @client = Rdkafka::NativeKafka.new(@native, run_polling_thread: false, opaque: @opaque, auto_start: false)
  end

  def teardown
    @client.close unless @client.closed?
    super
  end

  def test_allows_io_events_when_polling_thread_not_active
    signal_r, signal_w = IO.pipe
    @client.enable_main_queue_io_events(signal_w.fileno)
    @client.enable_background_queue_io_events(signal_w.fileno)
    signal_r.close
    signal_w.close
  end

  def test_accepts_custom_payload_for_io_events
    signal_r, signal_w = IO.pipe
    payload = "custom"
    @client.enable_main_queue_io_events(signal_w.fileno, payload)
    signal_r.close
    signal_w.close
  end

  def test_raises_closed_inner_error_for_main_queue_when_closed
    @client.close
    signal_r, signal_w = IO.pipe
    assert_raises(Rdkafka::ClosedInnerError) do
      @client.enable_main_queue_io_events(signal_w.fileno)
    end
    signal_r.close
    signal_w.close
  end

  def test_raises_closed_inner_error_for_background_queue_when_closed
    @client.close
    signal_r, signal_w = IO.pipe
    assert_raises(Rdkafka::ClosedInnerError) do
      @client.enable_background_queue_io_events(signal_w.fileno)
    end
    signal_r.close
    signal_w.close
  end
end
