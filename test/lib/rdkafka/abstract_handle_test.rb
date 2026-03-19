# frozen_string_literal: true

require_relative "../../test_helper"

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

describe Rdkafka::AbstractHandle do
  context "A subclass that does not implement the required methods" do
    it "raises an exception if operation_name is called" do
      e = assert_raises(RuntimeError) { BadTestHandle.new.operation_name }
      assert_match(/Must be implemented by subclass!/, e.message)
    end

    it "raises an exception if create_result is called" do
      e = assert_raises(RuntimeError) { BadTestHandle.new.create_result }
      assert_match(/Must be implemented by subclass!/, e.message)
    end
  end

  describe ".register and .remove" do
    before do
      @handle = TestHandle.new.tap do |handle|
        handle[:pending] = true
        handle[:response] = 0
        handle[:result] = -1
      end
    end

    it "registers and remove a delivery handle" do
      described_class.register(@handle)
      removed = described_class.remove(@handle.to_ptr.address)
      assert_equal @handle, removed
      assert_empty Rdkafka::AbstractHandle::REGISTRY
    end
  end

  describe "#pending?" do
    context "when true" do
      before do
        @handle = TestHandle.new.tap do |handle|
          handle[:pending] = true
          handle[:response] = 0
          handle[:result] = -1
        end
      end

      it "is true" do
        assert_equal true, @handle.pending?
      end
    end

    context "when not true" do
      before do
        @handle = TestHandle.new.tap do |handle|
          handle[:pending] = false
          handle[:response] = 0
          handle[:result] = -1
        end
      end

      it "is false" do
        assert_equal false, @handle.pending?
      end
    end
  end

  describe "#wait" do
    context "when pending_handle true" do
      before do
        @handle = TestHandle.new.tap do |handle|
          handle[:pending] = true
          handle[:response] = 0
          handle[:result] = -1
        end
      end

      it "waits until the timeout and then raise an error" do
        e = assert_raises(Rdkafka::AbstractHandle::WaitTimeoutError) {
          @handle.wait(max_wait_timeout_ms: 100)
        }
        assert_match(/test_operation/, e.message)
      end
    end

    context "when pending_handle false" do
      context "without error" do
        before do
          @handle = TestHandle.new.tap do |handle|
            handle[:pending] = false
            handle[:response] = 0
            handle[:result] = 1
          end
        end

        it "returns a result" do
          wait_result = @handle.wait
          assert_equal 1, wait_result
        end

        it "waits without a timeout" do
          wait_result = @handle.wait(max_wait_timeout_ms: nil)
          assert_equal 1, wait_result
        end
      end

      context "with error" do
        before do
          @handle = TestHandle.new.tap do |handle|
            handle[:pending] = false
            handle[:response] = 20
            handle[:result] = -1
          end
        end

        it "raises an rdkafka error" do
          assert_raises(Rdkafka::RdkafkaError) { @handle.wait }
        end
      end

      context "backwards compatibility with max_wait_timeout (seconds)" do
        before do
          @handle = TestHandle.new.tap do |handle|
            handle[:pending] = false
            handle[:response] = 0
            handle[:result] = 42
          end
        end

        it "works with max_wait_timeout (emits deprecation warning to stderr)" do
          wait_result = @handle.wait(max_wait_timeout: 5)
          assert_equal 42, wait_result
        end

        it "works with max_wait_timeout set to nil (wait forever)" do
          wait_result = @handle.wait(max_wait_timeout: nil)
          assert_equal 42, wait_result
        end

        it "properly converts seconds to milliseconds" do
          @handle[:pending] = true
          e = assert_raises(Rdkafka::AbstractHandle::WaitTimeoutError) {
            @handle.wait(max_wait_timeout: 0.1)
          }
          assert_match(/100 ms/, e.message)
        end

        it "uses new parameter when both are provided" do
          wait_result = @handle.wait(max_wait_timeout: 1, max_wait_timeout_ms: 5000)
          assert_equal 42, wait_result
        end

        it "timeouts based on max_wait_timeout_ms when both are provided" do
          @handle[:pending] = true
          e = assert_raises(Rdkafka::AbstractHandle::WaitTimeoutError) {
            @handle.wait(max_wait_timeout: 10, max_wait_timeout_ms: 100)
          }
          assert_match(/100 ms/, e.message)
        end
      end
    end
  end
end
