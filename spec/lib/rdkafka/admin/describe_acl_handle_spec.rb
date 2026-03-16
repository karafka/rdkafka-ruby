# frozen_string_literal: true

RSpec.describe Rdkafka::Admin::DescribeAclHandle do
  def resource_name
    @resource_name ||= TestTopics.unique
  end

  def build_handle(pending:)
    build_describe_acl_handle(pending: pending, resource_name: resource_name)
  end

  describe "#wait" do
    it "waits until the timeout and then raise an error" do
      handle = build_handle(pending: true)

      expect {
        handle.wait(max_wait_timeout_ms: 100)
      }.to raise_error Rdkafka::Admin::DescribeAclHandle::WaitTimeoutError, /describe acl/
    end

    context "when not pending anymore and no error" do
      it "returns a describe acl report" do
        handle = build_handle(pending: false)
        report = handle.wait

        expect(report.acls.length).to eq(1)
      end

      it "waits without a timeout" do
        handle = build_handle(pending: false)
        report = handle.wait(max_wait_timeout_ms: nil)

        expect(report.acls[0].matching_acl_resource_name).to eq(resource_name)
      end
    end
  end

  describe "#raise_error" do
    it "raises the appropriate error" do
      handle = build_handle(pending: false)

      expect {
        handle.raise_error
      }.to raise_exception(Rdkafka::RdkafkaError, /Success \(no_error\)/)
    end
  end
end
