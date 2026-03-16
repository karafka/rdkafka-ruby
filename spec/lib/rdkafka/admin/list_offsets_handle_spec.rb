# frozen_string_literal: true

RSpec.describe Rdkafka::Admin::ListOffsetsHandle do
  describe "#wait" do
    it "waits until the timeout and then raises an error" do
      handle = build_list_offsets_handle(pending: true)

      expect {
        handle.wait(max_wait_timeout_ms: 100)
      }.to raise_error Rdkafka::Admin::ListOffsetsHandle::WaitTimeoutError, /list offsets/
    end

    context "when not pending anymore and no error" do
      it "returns a list offsets report" do
        handle = build_list_offsets_handle(pending: false)
        report = handle.wait

        expect(report).to be_a(Rdkafka::Admin::ListOffsetsReport)
        expect(report.offsets).to eq([])
      end

      it "waits without a timeout" do
        handle = build_list_offsets_handle(pending: false)
        report = handle.wait(max_wait_timeout_ms: nil)

        expect(report.offsets).to eq([])
      end
    end
  end

  describe "#raise_error" do
    it "raises the appropriate error" do
      handle = build_list_offsets_handle(pending: false)

      expect {
        handle.raise_error
      }.to raise_exception(Rdkafka::RdkafkaError, /Success \(no_error\)/)
    end
  end
end
