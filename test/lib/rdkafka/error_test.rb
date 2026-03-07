# frozen_string_literal: true

require "test_helper"

class RdkafkaErrorTest < Minitest::Test
  def test_raises_type_error_for_nil_response
    assert_raises(TypeError) { Rdkafka::RdkafkaError.new(nil) }
  end

  def test_creates_error_with_message_prefix
    assert_equal "message prefix", Rdkafka::RdkafkaError.new(10, "message prefix").message_prefix
  end

  def test_creates_error_with_broker_message
    assert_equal "broker message", Rdkafka::RdkafkaError.new(10, broker_message: "broker message").broker_message
  end

  def test_creates_error_with_instance_name
    assert_equal "rdkafka#producer-1", Rdkafka::RdkafkaError.new(10, instance_name: "rdkafka#producer-1").instance_name
  end

  def test_defaults_instance_name_to_nil
    assert_nil Rdkafka::RdkafkaError.new(10).instance_name
  end

  def test_code_handles_invalid_response
    assert_equal :err_933975?, Rdkafka::RdkafkaError.new(933975).code
  end

  def test_code_returns_error_messages_from_rdkafka
    assert_equal :msg_size_too_large, Rdkafka::RdkafkaError.new(10).code
  end

  def test_code_strips_leading_underscore
    assert_equal :partition_eof, Rdkafka::RdkafkaError.new(-191).code
  end

  def test_to_s_handles_invalid_response
    assert_equal "Err-933975? (err_933975?)", Rdkafka::RdkafkaError.new(933975).to_s
  end

  def test_to_s_returns_error_messages_from_rdkafka
    assert_equal "Broker: Message size too large (msg_size_too_large)", Rdkafka::RdkafkaError.new(10).to_s
  end

  def test_to_s_adds_message_prefix
    assert_equal "Error explanation - Broker: Message size too large (msg_size_too_large)", Rdkafka::RdkafkaError.new(10, "Error explanation").to_s
  end

  def test_to_s_adds_instance_name
    assert_equal "Broker: Message size too large (msg_size_too_large) [rdkafka#producer-1]", Rdkafka::RdkafkaError.new(10, instance_name: "rdkafka#producer-1").to_s
  end

  def test_to_s_adds_both_prefix_and_instance_name
    assert_equal "Error explanation - Broker: Message size too large (msg_size_too_large) [rdkafka#producer-1]", Rdkafka::RdkafkaError.new(10, "Error explanation", instance_name: "rdkafka#producer-1").to_s
  end

  def test_message_handles_invalid_response
    assert_equal "Err-933975? (err_933975?)", Rdkafka::RdkafkaError.new(933975).message
  end

  def test_message_returns_error_messages_from_rdkafka
    assert_equal "Broker: Message size too large (msg_size_too_large)", Rdkafka::RdkafkaError.new(10).message
  end

  def test_message_adds_message_prefix
    assert_equal "Error explanation - Broker: Message size too large (msg_size_too_large)", Rdkafka::RdkafkaError.new(10, "Error explanation").message
  end

  def test_message_adds_instance_name
    assert_equal "Broker: Message size too large (msg_size_too_large) [rdkafka#producer-1]", Rdkafka::RdkafkaError.new(10, instance_name: "rdkafka#producer-1").message
  end

  def test_is_partition_eof_false_when_not_partition_eof
    refute_predicate Rdkafka::RdkafkaError.new(933975), :is_partition_eof?
  end

  def test_is_partition_eof_true_when_partition_eof
    assert_predicate Rdkafka::RdkafkaError.new(-191), :is_partition_eof?
  end

  def test_equals_another_error_with_same_content
    subject = Rdkafka::RdkafkaError.new(10, "Error explanation")

    assert_equal Rdkafka::RdkafkaError.new(10, "Error explanation"), subject
  end

  def test_does_not_equal_different_error_code
    subject = Rdkafka::RdkafkaError.new(10, "Error explanation")

    refute_equal Rdkafka::RdkafkaError.new(20, "Error explanation"), subject
  end

  def test_does_not_equal_different_message
    subject = Rdkafka::RdkafkaError.new(10, "Error explanation")

    refute_equal Rdkafka::RdkafkaError.new(10, "Different error explanation"), subject
  end

  def test_does_not_equal_no_message
    subject = Rdkafka::RdkafkaError.new(10, "Error explanation")

    refute_equal Rdkafka::RdkafkaError.new(10), subject
  end

  def test_does_not_equal_different_instance_name
    error_a = Rdkafka::RdkafkaError.new(10, instance_name: "rdkafka#producer-1")
    error_b = Rdkafka::RdkafkaError.new(10, instance_name: "rdkafka#producer-2")

    refute_equal error_b, error_a
  end

  def test_equals_same_instance_name
    error_a = Rdkafka::RdkafkaError.new(10, instance_name: "rdkafka#producer-1")
    error_b = Rdkafka::RdkafkaError.new(10, instance_name: "rdkafka#producer-1")

    assert_equal error_b, error_a
  end
end

class LibraryLoadErrorTest < Minitest::Test
  def test_is_subclass_of_base_error
    assert_kind_of Rdkafka::BaseError, Rdkafka::LibraryLoadError.new
  end

  def test_accepts_a_message
    error = Rdkafka::LibraryLoadError.new("test message")

    assert_equal "test message", error.message
  end
end
