# frozen_string_literal: true

RSpec.describe Rdkafka::AbstractHandle do
  subject do
    TestHandle.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      handle[:result] = result
    end
  end

  let(:response) { 0 }
  let(:result) { -1 }

  context "A subclass that does not implement the required methods" do
    class BadTestHandle < Rdkafka::AbstractHandle
      layout :pending, :bool,
        :response, :int
    end

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

  describe ".register and .remove" do
    let(:pending_handle) { true }

    it "registers and remove a delivery handle" do
      described_class.register(subject)
      removed = described_class.remove(subject.to_ptr.address)
      expect(removed).to eq subject
      expect(Rdkafka::AbstractHandle::REGISTRY).to be_empty
    end
  end

  describe "#pending?" do
    context "when true" do
      let(:pending_handle) { true }

      it "is true" do
        expect(subject.pending?).to be true
      end
    end

    context "when not true" do
      let(:pending_handle) { false }

      it "is false" do
        expect(subject.pending?).to be false
      end
    end
  end

  describe "#wait" do
    context "when pending_handle true" do
      let(:pending_handle) { true }

      it "waits until the timeout and then raise an error" do
        expect {
          subject.wait(max_wait_timeout_ms: 100)
        }.to raise_error Rdkafka::AbstractHandle::WaitTimeoutError, /test_operation/
      end
    end

    context "when pending_handle false" do
      let(:pending_handle) { false }

      context "without error" do
        let(:result) { 1 }

        it "returns a result" do
          wait_result = subject.wait
          expect(wait_result).to eq(result)
        end

        it "waits without a timeout" do
          wait_result = subject.wait(max_wait_timeout_ms: nil)
          expect(wait_result).to eq(result)
        end
      end

      context "with error" do
        let(:response) { 20 }

        it "raises an rdkafka error" do
          expect {
            subject.wait
          }.to raise_error Rdkafka::RdkafkaError
        end
      end

      context "backwards compatibility with max_wait_timeout (seconds)" do
        let(:result) { 42 }

        it "works with max_wait_timeout (emits deprecation warning to stderr)" do
          # Note: Deprecation warning is emitted but not tested here due to RSpec stderr capture complexity
          wait_result = subject.wait(max_wait_timeout: 5)
          expect(wait_result).to eq(result)
        end

        it "works with max_wait_timeout set to nil (wait forever)" do
          wait_result = subject.wait(max_wait_timeout: nil)
          expect(wait_result).to eq(result)
        end

        it "properlies convert seconds to milliseconds" do
          # Using a very short timeout to verify conversion
          subject[:pending] = true
          expect {
            subject.wait(max_wait_timeout: 0.1)
          }.to raise_error(Rdkafka::AbstractHandle::WaitTimeoutError, /100 ms/)
        end

        it "uses new parameter when both are provided" do
          # When both parameters provided, max_wait_timeout_ms takes precedence
          wait_result = subject.wait(max_wait_timeout: 1, max_wait_timeout_ms: 5000)
          expect(wait_result).to eq(result)
        end

        it "timeouts based on max_wait_timeout_ms when both are provided" do
          subject[:pending] = true
          # max_wait_timeout: 10 would be 10000ms, but max_wait_timeout_ms: 100 should take precedence
          expect {
            subject.wait(max_wait_timeout: 10, max_wait_timeout_ms: 100)
          }.to raise_error(Rdkafka::AbstractHandle::WaitTimeoutError, /100 ms/)
        end
      end
    end
  end
end
