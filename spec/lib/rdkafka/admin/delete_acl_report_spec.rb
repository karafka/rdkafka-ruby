# frozen_string_literal: true

RSpec.describe Rdkafka::Admin::DeleteAclReport do
  def resource_name
    @resource_name ||= TestTopics.unique
  end

  def build_report
    acl_ptr = build_acl_pointer(resource_name: resource_name)
    acls_array_ptr = build_acl_pointer_array(acl_ptr)
    Rdkafka::Admin::DeleteAclReport.new(matching_acls: acls_array_ptr, matching_acls_count: 1)
  end

  it "gets deleted acl resource type as Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC" do
    expect(build_report.deleted_acls[0].matching_acl_resource_type).to eq(Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC)
  end

  it "gets deleted acl resource name" do
    expect(build_report.deleted_acls[0].matching_acl_resource_name).to eq(resource_name)
  end

  it "gets deleted acl resource pattern type as Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL" do
    report = build_report
    expect(report.deleted_acls[0].matching_acl_resource_pattern_type).to eq(Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL)
    expect(report.deleted_acls[0].matching_acl_pattern_type).to eq(Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL)
  end

  it "gets deleted acl principal as User:anonymous" do
    expect(build_report.deleted_acls[0].matching_acl_principal).to eq("User:anonymous")
  end

  it "gets deleted acl host as *" do
    expect(build_report.deleted_acls[0].matching_acl_host).to eq("*")
  end

  it "gets deleted acl operation as Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ" do
    expect(build_report.deleted_acls[0].matching_acl_operation).to eq(Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ)
  end

  it "gets deleted acl permission_type as Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW" do
    expect(build_report.deleted_acls[0].matching_acl_permission_type).to eq(Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW)
  end
end
