# frozen_string_literal: true

RSpec.describe Rdkafka::AbstractHandle do
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

  def build_handle(pending:, response: 0, result: -1)
    TestHandle.new.tap do |handle|
      handle[:pending] = pending
      handle[:response] = response
      handle[:result] = result
    end
  end

  context "A subclass that does not implement the required methods" do
    it "raises an exception if operation_name is called" do
      expect {
        BadTestHandle.new.operation_name
      }.to raise_exception(RuntimeError, /Must be implemented by subclass!/)
    end

    it "raises an exception if create_result is called" do
      expect {
        BadTestHandle.new.create_result
      }.to raise_exception(RuntimeError, /Must be implemented by subclass!/)
    end
  end

  describe ".register and .remove" do
    it "registers and remove a delivery handle" do
      handle = build_handle(pending: true)
      described_class.register(handle)
      removed = described_class.remove(handle.to_ptr.address)
      expect(removed).to eq handle
      expect(Rdkafka::AbstractHandle::REGISTRY).to be_empty
    end
  end

  describe "#pending?" do
    context "when true" do
      it "is true" do
        expect(build_handle(pending: true).pending?).to be true
      end
    end

    context "when not true" do
      it "is false" do
        expect(build_handle(pending: false).pending?).to be false
      end
    end
  end

  describe "#wait" do
    context "when pending_handle true" do
      it "waits until the timeout and then raise an error" do
        handle = build_handle(pending: true)

        expect {
          handle.wait(max_wait_timeout_ms: 100)
        }.to raise_error Rdkafka::AbstractHandle::WaitTimeoutError, /test_operation/
      end
    end

    context "when pending_handle false" do
      context "without error" do
        it "returns a result" do
          handle = build_handle(pending: false, result: 1)
          wait_result = handle.wait
          expect(wait_result).to eq(1)
        end

        it "waits without a timeout" do
          handle = build_handle(pending: false, result: 1)
          wait_result = handle.wait(max_wait_timeout_ms: nil)
          expect(wait_result).to eq(1)
        end
      end

      context "with error" do
        it "raises an rdkafka error" do
          handle = build_handle(pending: false, response: 20)

          expect {
            handle.wait
          }.to raise_error Rdkafka::RdkafkaError
        end
      end

      context "backwards compatibility with max_wait_timeout (seconds)" do
        it "works with max_wait_timeout (emits deprecation warning to stderr)" do
          handle = build_handle(pending: false, result: 42)
          # Note: Deprecation warning is emitted but not tested here due to RSpec stderr capture complexity
          wait_result = handle.wait(max_wait_timeout: 5)
          expect(wait_result).to eq(42)
        end

        it "works with max_wait_timeout set to nil (wait forever)" do
          handle = build_handle(pending: false, result: 42)
          wait_result = handle.wait(max_wait_timeout: nil)
          expect(wait_result).to eq(42)
        end

        it "properlies convert seconds to milliseconds" do
          handle = build_handle(pending: true, result: 42)
          # Using a very short timeout to verify conversion
          expect {
            handle.wait(max_wait_timeout: 0.1)
          }.to raise_error(Rdkafka::AbstractHandle::WaitTimeoutError, /100 ms/)
        end

        it "uses new parameter when both are provided" do
          handle = build_handle(pending: false, result: 42)
          # When both parameters provided, max_wait_timeout_ms takes precedence
          wait_result = handle.wait(max_wait_timeout: 1, max_wait_timeout_ms: 5000)
          expect(wait_result).to eq(42)
        end

        it "timeouts based on max_wait_timeout_ms when both are provided" do
          handle = build_handle(pending: true, result: 42)
          # max_wait_timeout: 10 would be 10000ms, but max_wait_timeout_ms: 100 should take precedence
          expect {
            handle.wait(max_wait_timeout: 10, max_wait_timeout_ms: 100)
          }.to raise_error(Rdkafka::AbstractHandle::WaitTimeoutError, /100 ms/)
        end
      end
    end
  end
end
