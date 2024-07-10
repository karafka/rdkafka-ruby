require "spec_helper"

describe Rdkafka::Producer::DeliveryHandle do
  let(:response) { 0 }

  subject do
    Rdkafka::Producer::DeliveryHandle.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      handle[:partition] = 2
      handle[:offset] = 100
    end
  end

  describe "#wait" do
    let(:pending_handle) { true }

    it "should wait until the timeout and then raise an error" do
      expect {
        subject.wait(max_wait_timeout: 0.1)
      }.to raise_error Rdkafka::Producer::DeliveryHandle::WaitTimeoutError, /delivery/
    end

    context "when not pending anymore and no error" do
      let(:pending_handle) { false }

      it "should return a delivery report" do
        report = subject.wait

        expect(report.partition).to eq(2)
        expect(report.offset).to eq(100)
      end

      it "should wait without a timeout" do
        report = subject.wait(max_wait_timeout: nil)

        expect(report.partition).to eq(2)
        expect(report.offset).to eq(100)
      end
    end
  end
end
