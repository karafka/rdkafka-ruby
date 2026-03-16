# frozen_string_literal: true

RSpec.describe Rdkafka::Admin::CreateAclHandle do
  describe "#wait" do
    it "waits until the timeout and then raise an error" do
      handle = build_create_acl_handle(pending: true)

      expect {
        handle.wait(max_wait_timeout_ms: 100)
      }.to raise_error Rdkafka::Admin::CreateAclHandle::WaitTimeoutError, /create acl/
    end

    context "when not pending anymore and no error" do
      it "returns a create acl report" do
        handle = build_create_acl_handle(pending: false)
        report = handle.wait

        expect(report.rdkafka_response_string).to eq("")
      end

      it "waits without a timeout" do
        handle = build_create_acl_handle(pending: false)
        report = handle.wait(max_wait_timeout_ms: nil)

        expect(report.rdkafka_response_string).to eq("")
      end
    end
  end

  describe "#raise_error" do
    it "raises the appropriate error" do
      handle = build_create_acl_handle(pending: false)

      expect {
        handle.raise_error
      }.to raise_exception(Rdkafka::RdkafkaError, /Success \(no_error\)/)
    end
  end
end
