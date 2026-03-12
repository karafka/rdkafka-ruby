# frozen_string_literal: true

require_relative "../../support/test_handles"

describe Rdkafka::AbstractHandle do
  let(:response) { 0 }
  let(:result) { -1 }
  let(:pending_handle) { true }

  subject do
    TestHandle.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      handle[:result] = result
    end
  end

  describe "a subclass that does not implement the required methods" do
    it "raises an exception if operation_name is called" do
      error = assert_raises(RuntimeError) { BadTestHandle.new.operation_name }
      assert_match(/Must be implemented by subclass!/, error.message)
    end

    it "raises an exception if create_result is called" do
      error = assert_raises(RuntimeError) { BadTestHandle.new.create_result }
      assert_match(/Must be implemented by subclass!/, error.message)
    end
  end

  describe ".register and .remove" do
    let(:pending_handle) { true }

    it "registers and removes a delivery handle" do
      Rdkafka::AbstractHandle.register(subject)
      removed = Rdkafka::AbstractHandle.remove(subject.to_ptr.address)

      assert_equal subject, removed
      assert_empty Rdkafka::AbstractHandle::REGISTRY
    end
  end

  describe "#pending?" do
    describe "when true" do
      let(:pending_handle) { true }

      it "is true" do
        assert_predicate subject, :pending?
      end
    end

    describe "when not true" do
      let(:pending_handle) { false }

      it "is false" do
        refute_predicate subject, :pending?
      end
    end
  end

  describe "#wait" do
    describe "when pending_handle true" do
      let(:pending_handle) { true }

      it "waits until the timeout and then raises an error" do
        error = assert_raises(Rdkafka::AbstractHandle::WaitTimeoutError) do
          subject.wait(max_wait_timeout_ms: 100)
        end
        assert_match(/test_operation/, error.message)
      end
    end

    describe "when pending_handle false" do
      let(:pending_handle) { false }

      describe "without error" do
        let(:result) { 1 }

        it "returns a result" do
          assert_equal 1, subject.wait
        end

        it "waits without a timeout" do
          assert_equal 1, subject.wait(max_wait_timeout_ms: nil)
        end
      end

      describe "with error" do
        let(:response) { 20 }

        it "raises an rdkafka error" do
          assert_raises(Rdkafka::RdkafkaError) { subject.wait }
        end
      end

      describe "backwards compatibility with max_wait_timeout (seconds)" do
        let(:result) { 42 }

        it "works with max_wait_timeout" do
          assert_equal 42, subject.wait(max_wait_timeout: 5)
        end

        it "works with max_wait_timeout set to nil" do
          assert_equal 42, subject.wait(max_wait_timeout: nil)
        end

        it "properly converts seconds to milliseconds" do
          subject[:pending] = true
          error = assert_raises(Rdkafka::AbstractHandle::WaitTimeoutError) do
            subject.wait(max_wait_timeout: 0.1)
          end
          assert_match(/100 ms/, error.message)
        end

        it "uses new parameter when both are provided" do
          assert_equal 42, subject.wait(max_wait_timeout: 1, max_wait_timeout_ms: 5000)
        end

        it "timeouts based on max_wait_timeout_ms when both are provided" do
          subject[:pending] = true
          error = assert_raises(Rdkafka::AbstractHandle::WaitTimeoutError) do
            subject.wait(max_wait_timeout: 10, max_wait_timeout_ms: 100)
          end
          assert_match(/100 ms/, error.message)
        end
      end
    end
  end
end
