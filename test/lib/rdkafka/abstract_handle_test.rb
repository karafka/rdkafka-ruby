# frozen_string_literal: true

require "test_helper"

class BadTestHandle < Rdkafka::AbstractHandle
  layout :pending, :bool,
    :response, :int
end

class TestHandle < Rdkafka::AbstractHandle
  layout :pending, :bool,
    :response, :int,
    :result, :int

  def operation_name
    "test_operation"
  end

  def create_result
    self[:result]
  end
end

class AbstractHandleTest < Minitest::Test
  def new_subject(pending_handle:, response: 0, result: -1)
    TestHandle.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      handle[:result] = result
    end
  end

  def test_subclass_raises_on_operation_name
    error = assert_raises(RuntimeError) { BadTestHandle.new.operation_name }
    assert_match(/Must be implemented by subclass!/, error.message)
  end

  def test_subclass_raises_on_create_result
    error = assert_raises(RuntimeError) { BadTestHandle.new.create_result }
    assert_match(/Must be implemented by subclass!/, error.message)
  end

  def test_register_and_remove
    subject = new_subject(pending_handle: true)
    Rdkafka::AbstractHandle.register(subject)
    removed = Rdkafka::AbstractHandle.remove(subject.to_ptr.address)

    assert_equal subject, removed
    assert_empty Rdkafka::AbstractHandle::REGISTRY
  end

  def test_pending_when_true
    subject = new_subject(pending_handle: true)

    assert_predicate subject, :pending?
  end

  def test_pending_when_false
    subject = new_subject(pending_handle: false)

    refute_predicate subject, :pending?
  end

  def test_wait_raises_timeout_when_pending
    subject = new_subject(pending_handle: true)
    error = assert_raises(Rdkafka::AbstractHandle::WaitTimeoutError) do
      subject.wait(max_wait_timeout_ms: 100)
    end
    assert_match(/test_operation/, error.message)
  end

  def test_wait_returns_result_when_not_pending
    subject = new_subject(pending_handle: false, result: 1)

    assert_equal 1, subject.wait
  end

  def test_wait_without_timeout
    subject = new_subject(pending_handle: false, result: 1)

    assert_equal 1, subject.wait(max_wait_timeout_ms: nil)
  end

  def test_wait_raises_rdkafka_error_on_error_response
    subject = new_subject(pending_handle: false, response: 20)
    assert_raises(Rdkafka::RdkafkaError) { subject.wait }
  end

  def test_backwards_compat_max_wait_timeout_seconds
    subject = new_subject(pending_handle: false, result: 42)

    assert_equal 42, subject.wait(max_wait_timeout: 5)
  end

  def test_backwards_compat_max_wait_timeout_nil
    subject = new_subject(pending_handle: false, result: 42)

    assert_equal 42, subject.wait(max_wait_timeout: nil)
  end

  def test_backwards_compat_converts_seconds_to_milliseconds
    subject = new_subject(pending_handle: true)
    error = assert_raises(Rdkafka::AbstractHandle::WaitTimeoutError) do
      subject.wait(max_wait_timeout: 0.1)
    end
    assert_match(/100 ms/, error.message)
  end

  def test_new_param_takes_precedence_when_both_provided
    subject = new_subject(pending_handle: false, result: 42)

    assert_equal 42, subject.wait(max_wait_timeout: 1, max_wait_timeout_ms: 5000)
  end

  def test_timeout_based_on_ms_when_both_provided
    subject = new_subject(pending_handle: true)
    error = assert_raises(Rdkafka::AbstractHandle::WaitTimeoutError) do
      subject.wait(max_wait_timeout: 10, max_wait_timeout_ms: 100)
    end
    assert_match(/100 ms/, error.message)
  end
end
