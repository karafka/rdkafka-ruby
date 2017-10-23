require "spec_helper"

describe Rdkafka::Producer::DeliveryHandle do
  let(:response) { 0 }
  subject do
    Rdkafka::Producer::DeliveryHandle.new.tap do |handle|
      handle[:pending] = pending
      handle[:response] = response
      handle[:partition] = 2
      handle[:offset] = 100
    end
  end

  describe "#pending?" do
    context "when true" do
      let(:pending) { true }

      it "should be true" do
        expect(subject.pending?).to be true
      end
    end

    context "when not true" do
      let(:pending) { false }

      it "should be false" do
        expect(subject.pending?).to be false
      end
    end
  end

  describe "#wait" do
    let(:pending) { true }

    it "should wait until the timeout and then raise an error" do
      expect {
        subject.wait(0.1)
      }.to raise_error Rdkafka::Producer::DeliveryHandle::WaitTimeoutError
    end

    context "when not pending anymore and no error" do
      let(:pending) { false }

      it "should return a delivery report" do
        report = subject.wait

        expect(report.partition).to eq(2)
        expect(report.offset).to eq(100)
      end

      it "should wait without a timeout" do
        report = subject.wait(nil)

        expect(report.partition).to eq(2)
        expect(report.offset).to eq(100)
      end
    end

    context "when not pending anymore and there was an error" do
      let(:pending) { false }
      let(:response) { 20 }

      it "should raise an rdkafka error" do
        expect {
          subject.wait
        }.to raise_error Rdkafka::RdkafkaError
      end
    end
  end
end
