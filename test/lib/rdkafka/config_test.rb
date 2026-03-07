# frozen_string_literal: true

require "test_helper"

class ConfigTest < Minitest::Test
  def test_has_default_logger
    assert_kind_of Logger, Rdkafka::Config.logger
  end

  def test_sets_the_logger
    logger = Logger.new($stdout)

    refute_equal logger, Rdkafka::Config.logger
    Rdkafka::Config.logger = logger

    assert_equal logger, Rdkafka::Config.logger
  end

  def test_does_not_accept_nil_logger
    assert_raises(Rdkafka::Config::NoLoggerError) do
      Rdkafka::Config.logger = nil
    end
  end

  def test_supports_logging_queue
    log = StringIO.new
    Rdkafka::Config.logger = Logger.new(log)
    Rdkafka::Config.ensure_log_thread

    Rdkafka::Config.log_queue << [Logger::FATAL, "I love testing"]
    20.times do
      break if log.string != ""
      sleep 0.05
    end

    assert_includes log.string, "FATAL -- : I love testing"
  end

  unless RUBY_PLATFORM == "java"
    def test_starts_new_logger_thread_after_fork
      reader, writer = IO.pipe

      pid = fork do
        $stdout.reopen(writer)
        Rdkafka::Config.logger = Logger.new($stdout)
        reader.close
        producer = rdkafka_producer_config(debug: "all").producer
        producer.close
        writer.close
        sleep(1)
      end

      writer.close
      Process.wait(pid)
      output = reader.read

      assert_operator output.split("\n").size, :>=, 20
    end
  end

  def test_statistics_callback_with_proc
    Rdkafka::Config.statistics_callback = lambda do |stats|
    end

    assert_respond_to Rdkafka::Config.statistics_callback, :call
  end

  def test_statistics_callback_with_callable_object
    callback = Class.new do
      def call(stats)
      end
    end
    Rdkafka::Config.statistics_callback = callback.new

    assert_respond_to Rdkafka::Config.statistics_callback, :call
  end

  def test_statistics_callback_rejects_non_callable
    assert_raises(TypeError) do
      Rdkafka::Config.statistics_callback = "a string"
    end
  end

  def test_error_callback_with_proc
    Rdkafka::Config.error_callback = lambda do |error|
    end

    assert_respond_to Rdkafka::Config.error_callback, :call
  end

  def test_error_callback_with_callable_object
    callback = Class.new do
      def call(stats)
      end
    end
    Rdkafka::Config.error_callback = callback.new

    assert_respond_to Rdkafka::Config.error_callback, :call
  end

  def test_error_callback_rejects_non_callable
    assert_raises(TypeError) do
      Rdkafka::Config.error_callback = "a string"
    end
  end

  def test_oauthbearer_callback_with_proc
    Rdkafka::Config.oauthbearer_token_refresh_callback = lambda do |config, client_name|
    end

    assert_respond_to Rdkafka::Config.oauthbearer_token_refresh_callback, :call
  end

  def test_oauthbearer_callback_with_callable_object
    callback = Class.new do
      def call(config, client_name)
      end
    end
    Rdkafka::Config.oauthbearer_token_refresh_callback = callback.new

    assert_respond_to Rdkafka::Config.oauthbearer_token_refresh_callback, :call
  end

  def test_oauthbearer_callback_rejects_non_callable
    assert_raises(TypeError) do
      Rdkafka::Config.oauthbearer_token_refresh_callback = "not a callback"
    end
  end

  def test_stores_configuration
    config = Rdkafka::Config.new
    config[:key] = "value"

    assert_equal "value", config[:key]
  end

  def test_uses_default_configuration
    config = Rdkafka::Config.new

    assert_nil config[:"api.version.request"]
  end

  def test_creates_consumer_with_valid_config
    consumer = rdkafka_consumer_config.consumer

    assert_kind_of Rdkafka::Consumer, consumer
    consumer.close
  end

  def test_creates_consumer_with_consumer_poll_set_false
    config = rdkafka_consumer_config
    config.consumer_poll_set = false
    consumer = config.consumer

    assert_kind_of Rdkafka::Consumer, consumer
    consumer.close
  end

  def test_raises_error_creating_consumer_with_invalid_config
    config = Rdkafka::Config.new("invalid.key" => "value")
    error = assert_raises(Rdkafka::Config::ConfigError) { config.consumer }
    assert_equal 'No such configuration property: "invalid.key"', error.message
  end

  def test_raises_error_creating_consumer_with_nil_key
    config = Rdkafka::Config.new(nil => "value")
    error = assert_raises(Rdkafka::Config::ConfigError) { config.consumer }
    assert_equal 'No such configuration property: ""', error.message
  end

  def test_treats_nil_value_as_blank
    config = Rdkafka::Config.new("security.protocol" => nil)
    error = assert_raises(Rdkafka::Config::ConfigError) do
      config.consumer
      config.producer
    end
    assert_equal 'Configuration property "security.protocol" cannot be set to empty value', error.message
  end

  def test_creates_producer_with_valid_config
    producer = rdkafka_consumer_config.producer

    assert_kind_of Rdkafka::Producer, producer
    producer.close
  end

  def test_raises_error_creating_producer_with_invalid_config
    config = Rdkafka::Config.new("invalid.key" => "value")
    error = assert_raises(Rdkafka::Config::ConfigError) { config.producer }
    assert_equal 'No such configuration property: "invalid.key"', error.message
  end

  def test_allows_string_partitioner_key
    config = Rdkafka::Config.new("partitioner" => "murmur2")
    producer = config.producer

    assert_kind_of Rdkafka::Producer, producer
    producer.close
  end

  def test_allows_zstd_compression
    config = Rdkafka::Config.new("compression.codec" => "zstd")
    begin
      producer = config.producer

      assert_kind_of Rdkafka::Producer, producer
      producer.close
    rescue Rdkafka::Config::ConfigError
      skip "Zstd compression not supported on this machine"
    end
  end

  def test_raises_error_when_consumer_creation_fails
    config = Rdkafka::Config.new(
      "security.protocol" => "SSL",
      "ssl.ca.location" => "/nonsense"
    )
    error = assert_raises(Rdkafka::Config::ClientCreationError) { config.consumer }
    assert_match(/ssl.ca.location failed(.*)/, error.message)
  end

  def test_raises_error_when_producer_creation_fails
    config = Rdkafka::Config.new(
      "security.protocol" => "SSL",
      "ssl.ca.location" => "/nonsense"
    )
    error = assert_raises(Rdkafka::Config::ClientCreationError) { config.producer }
    assert_match(/ssl.ca.location failed(.*)/, error.message)
  end
end
