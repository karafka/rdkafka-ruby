# frozen_string_literal: true

# This integration test verifies that every result of a multi-item admin request is read correctly.
#
# The topic, group and ACL result builders walked librdkafka's result arrays with
# `array_pointer + index`, but `FFI::Pointer#+` advances by bytes, not pointer width. Only the first
# element was read correctly; every later one came from a misaligned address, which reads garbage
# or crashes. The public `Admin` methods always send a single item, so this sends real two-item
# CreateTopics, DeleteGroups, CreateAcls and DeleteAcls requests through the same background queue,
# handles and callback handlers, and records what each real builder returns.
#
# Requires a running Kafka broker at localhost:9092 with an ACL authorizer enabled.
#
# Exit codes:
# - 0: every result of every request was read correctly
# - 1: a result was missing or wrong (a crash also means the results were misread)

require "rdkafka"
require "securerandom"

$stdout.sync = true

Callbacks = Rdkafka::Callbacks
Bindings = Rdkafka::Bindings

captured = {}

{
  Callbacks::TopicResult => :create_topic_results_from_array,
  Callbacks::GroupResult => :create_group_results_from_array,
  Callbacks::CreateAclResult => :create_acl_results_from_array,
  Callbacks::DeleteAclResult => :delete_acl_results_from_array
}.each do |result_class, builder|
  result_class.singleton_class.prepend(
    Module.new do
      define_method(builder) do |count, array_pointer|
        super(count, array_pointer).tap { |results| captured[builder] = results }
      end
    end
  )
end

admin = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").admin
native_kafka = admin.instance_variable_get(:@native_kafka)

# Enqueues a multi-item request the way the single-item `Admin` methods do and waits for it
def dispatch(native_kafka, handle_class, operation, items)
  items_ptr = FFI::MemoryPointer.new(:pointer, items.size)
  items_ptr.write_array_of_pointer(items)

  queue_ptr = native_kafka.with_inner { |inner| Bindings.rd_kafka_queue_get_background(inner) }
  handle = handle_class.new
  handle[:pending] = true
  handle[:response] = Bindings::RD_KAFKA_PARTITION_UA
  handle_class.register(handle)

  options_ptr = native_kafka.with_inner { |inner| Bindings.rd_kafka_AdminOptions_new(inner, operation) }
  Bindings.rd_kafka_AdminOptions_set_opaque(options_ptr, handle.to_ptr)

  native_kafka.with_inner do |inner|
    yield(inner, items_ptr, items.size, options_ptr, queue_ptr)
  end

  Bindings.rd_kafka_AdminOptions_destroy(options_ptr)
  Bindings.rd_kafka_queue_destroy(queue_ptr)

  begin
    handle.wait(max_wait_timeout_ms: 15_000)
  rescue Rdkafka::RdkafkaError
    # Only the first result drives the handle; each result is checked separately below
    nil
  end
end

def acl_binding(topic, filter: false)
  error_buffer = FFI::MemoryPointer.from_string(" " * 256)
  args = [
    Bindings::RD_KAFKA_RESOURCE_TOPIC,
    FFI::MemoryPointer.from_string(topic),
    Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL,
    FFI::MemoryPointer.from_string("User:multi-item"),
    FFI::MemoryPointer.from_string("*"),
    Bindings::RD_KAFKA_ACL_OPERATION_READ,
    Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW,
    error_buffer,
    256
  ]

  filter ? Bindings.rd_kafka_AclBindingFilter_new(*args) : Bindings.rd_kafka_AclBinding_new(*args)
end

failures = []
check = lambda do |label, actual, expected|
  if actual == expected
    puts "ok: #{label}"
  else
    failures << label
    puts "FAIL: #{label}: expected #{expected.inspect}, got #{actual.inspect}"
  end
end

topics = Array.new(2) { "it-multi-item-#{SecureRandom.hex(6)}" }
groups = Array.new(2) { "it-multi-item-#{SecureRandom.hex(6)}" }

new_topics = topics.map do |topic|
  Bindings.rd_kafka_NewTopic_new(FFI::MemoryPointer.from_string(topic), 1, 1, FFI::MemoryPointer.new(256), 256)
end
dispatch(native_kafka, Rdkafka::Admin::CreateTopicHandle, Bindings::RD_KAFKA_ADMIN_OP_CREATETOPICS, new_topics) do |*args|
  Bindings.rd_kafka_CreateTopics(*args)
end
new_topics.each { |topic| Bindings.rd_kafka_NewTopic_destroy(topic) }
check.call("CreateTopics result names", captured[:create_topic_results_from_array]&.map { |result| result.result_name.read_string }, topics)

delete_groups = groups.map { |group| Bindings.rd_kafka_DeleteGroup_new(FFI::MemoryPointer.from_string(group)) }
dispatch(native_kafka, Rdkafka::Admin::DeleteGroupsHandle, Bindings::RD_KAFKA_ADMIN_OP_DELETEGROUPS, delete_groups) do |*args|
  Bindings.rd_kafka_DeleteGroups(*args)
end
delete_groups.each { |group| Bindings.rd_kafka_DeleteGroup_destroy(group) }
check.call("DeleteGroups result names", captured[:create_group_results_from_array]&.map { |result| result.result_name.read_string }, groups)

bindings = topics.map { |topic| acl_binding(topic) }
dispatch(native_kafka, Rdkafka::Admin::CreateAclHandle, Bindings::RD_KAFKA_ADMIN_OP_CREATEACLS, bindings) do |*args|
  Bindings.rd_kafka_CreateAcls(*args)
end
bindings.each { |binding| Bindings.rd_kafka_AclBinding_destroy(binding) }
check.call(
  "CreateAcls result errors",
  captured[:create_acl_results_from_array]&.map(&:result_error),
  [Bindings::RD_KAFKA_RESP_ERR_NO_ERROR] * 2
)

filters = topics.map { |topic| acl_binding(topic, filter: true) }
dispatch(native_kafka, Rdkafka::Admin::DeleteAclHandle, Bindings::RD_KAFKA_ADMIN_OP_DELETEACLS, filters) do |*args|
  Bindings.rd_kafka_DeleteAcls(*args)
end
filters.each { |filter| Bindings.rd_kafka_AclBinding_destroy(filter) }
check.call(
  "DeleteAcls matches per filter",
  captured[:delete_acl_results_from_array]&.map(&:matching_acls_count),
  [1, 1]
)

topics.each do |topic|
  admin.delete_topic(topic).wait(max_wait_timeout_ms: 15_000)
rescue Rdkafka::RdkafkaError
  nil
end
admin.close

exit(failures.empty? ? 0 : 1)
