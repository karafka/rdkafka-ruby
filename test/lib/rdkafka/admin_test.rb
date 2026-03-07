# frozen_string_literal: true

require "test_helper"
require "ostruct"

class AdminTest < Minitest::Test
  def setup
    super
    @config = rdkafka_config
    @topic_name = "test-topic-#{SecureRandom.uuid}"
    @topic_partition_count = 3
    @topic_replication_factor = 1
    @topic_config = { "cleanup.policy" => "compact", "min.cleanable.dirty.ratio" => 0.8 }
    @invalid_topic_config = { "cleeeeenup.policee" => "campact" }
    @group_name = "test-group-#{SecureRandom.uuid}"
    @resource_name = TestTopics.unique
    @resource_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC
    @resource_pattern_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL
    @principal = "User:anonymous"
    @host = "*"
    @operation = Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ
    @permission_type = Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW
  end

  attr_reader :config

  attr_reader :topic_name

  attr_reader :topic_partition_count

  attr_reader :topic_replication_factor

  attr_reader :topic_config

  attr_reader :invalid_topic_config

  attr_reader :group_name

  attr_reader :resource_name

  attr_reader :resource_type

  attr_reader :resource_pattern_type

  attr_reader :principal

  attr_reader :host

  attr_reader :operation

  attr_reader :permission_type

  def admin
    @admin ||= config.admin
  end

  def teardown
    # Registry should always end up being empty
    assert_empty Rdkafka::Admin::CreateTopicHandle::REGISTRY
    assert_empty Rdkafka::Admin::CreatePartitionsHandle::REGISTRY
    assert_empty Rdkafka::Admin::DescribeAclHandle::REGISTRY
    assert_empty Rdkafka::Admin::CreateAclHandle::REGISTRY
    assert_empty Rdkafka::Admin::DeleteAclHandle::REGISTRY
    assert_empty Rdkafka::Admin::ListOffsetsHandle::REGISTRY
    admin&.close
    super
  end

  # describe_errors tests

  def test_describe_errors_size
    errors = Rdkafka::Admin.describe_errors

    assert_equal 172, errors.size
  end

  def test_describe_errors_negative_code
    errors = Rdkafka::Admin.describe_errors

    assert_equal({ code: -184, description: "Local: Queue full", name: "_QUEUE_FULL" }, errors[-184])
  end

  def test_describe_errors_positive_code
    errors = Rdkafka::Admin.describe_errors

    assert_equal({ code: 21, description: "Broker: Invalid required acks value", name: "INVALID_REQUIRED_ACKS" }, errors[21])
  end

  # admin without auto-start tests

  def test_admin_without_auto_start_can_start_and_close
    admin_no_start = config.admin(native_kafka_auto_start: false)
    admin_no_start.start
    admin_no_start.close
  end

  def test_admin_without_auto_start_can_close_without_starting
    admin_no_start = config.admin(native_kafka_auto_start: false)
    admin_no_start.close
  end

  # create_topic tests

  def test_create_topic_with_invalid_topic_name
    create_topic_handle = admin.create_topic("[!@#]", topic_partition_count, topic_replication_factor)
    ex = assert_raises(Rdkafka::RdkafkaError) {
      create_topic_handle.wait(max_wait_timeout_ms: 15_000)
    }
    assert_kind_of Rdkafka::RdkafkaError, ex
    assert_match(/Broker: Invalid topic \(topic_exception\)/, ex.message)
    assert_match(/Topic name.*is invalid: .* contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'/, ex.broker_message)
  end

  def test_create_topic_with_existing_topic_name
    existing_topic_name = TestTopics.empty_test_topic
    create_topic_handle = admin.create_topic(existing_topic_name, topic_partition_count, topic_replication_factor)
    ex = assert_raises(Rdkafka::RdkafkaError) {
      create_topic_handle.wait(max_wait_timeout_ms: 15_000)
    }
    assert_kind_of Rdkafka::RdkafkaError, ex
    assert_match(/Broker: Topic already exists \(topic_already_exists\)/, ex.message)
    assert_match(/Topic '#{Regexp.escape(TestTopics.empty_test_topic)}' already exists/, ex.broker_message)
  end

  def test_create_topic_with_invalid_partition_count
    error = assert_raises(Rdkafka::Config::ConfigError) {
      admin.create_topic(topic_name, -999, topic_replication_factor)
    }
    assert_match(/num_partitions out of expected range/, error.message)
  end

  def test_create_topic_with_invalid_replication_factor
    error = assert_raises(Rdkafka::Config::ConfigError) {
      admin.create_topic(topic_name, topic_partition_count, -2)
    }
    assert_match(/replication_factor out of expected range/, error.message)
  end

  def test_create_topic_with_invalid_topic_configuration
    create_topic_handle = admin.create_topic(topic_name, topic_partition_count, topic_replication_factor, invalid_topic_config)
    error = assert_raises(Rdkafka::RdkafkaError) {
      create_topic_handle.wait(max_wait_timeout_ms: 15_000)
    }
    assert_match(/Broker: Configuration is invalid \(invalid_config\)/, error.message)
  end

  def test_create_topic_edge_case_null_background_queue
    Rdkafka::Bindings.stub(:rd_kafka_queue_get_background, FFI::Pointer::NULL) do
      error = assert_raises(Rdkafka::Config::ConfigError) {
        admin.create_topic(topic_name, topic_partition_count, topic_replication_factor)
      }
      assert_match(/rd_kafka_queue_get_background was NULL/, error.message)
    end
  end

  def test_create_topic_edge_case_create_topics_raises
    Rdkafka::Bindings.stub(:rd_kafka_CreateTopics, proc { raise "oops" }) do
      error = assert_raises(RuntimeError) {
        admin.create_topic(topic_name, topic_partition_count, topic_replication_factor)
      }
      assert_match(/oops/, error.message)
    end
  end

  def test_create_topic_creates_a_topic
    create_topic_handle = admin.create_topic(topic_name, topic_partition_count, topic_replication_factor, topic_config)
    create_topic_report = create_topic_handle.wait(max_wait_timeout_ms: 15_000)

    assert_nil create_topic_report.error_string
    assert_equal topic_name, create_topic_report.result_name
  end

  # describe_configs tests

  def test_describe_configs_existing_topic
    admin.create_topic(topic_name, 2, 1).wait
    sleep(1)

    resources = [{ resource_type: 2, resource_name: topic_name }]
    resources_results = admin.describe_configs(resources).wait.resources

    assert_equal 1, resources_results.size
    assert_equal 2, resources_results.first.type
    assert_equal topic_name, resources_results.first.name
    assert_operator resources_results.first.configs.size, :>, 25
    assert_equal "compression.type", resources_results.first.configs.first.name
    assert_equal "producer", resources_results.first.configs.first.value
    refute_empty resources_results.first.configs.map(&:synonyms)
  end

  def test_describe_configs_non_existing_topic
    admin.create_topic(topic_name, 2, 1).wait
    sleep(1)

    resources = [{ resource_type: 2, resource_name: SecureRandom.uuid }]
    assert_raises(Rdkafka::RdkafkaError) {
      admin.describe_configs(resources).wait.resources
    }
  end

  def test_describe_configs_existing_and_non_existing_topics
    admin.create_topic(topic_name, 2, 1).wait
    sleep(1)

    resources = [
      { resource_type: 2, resource_name: topic_name },
      { resource_type: 2, resource_name: SecureRandom.uuid }
    ]
    assert_raises(Rdkafka::RdkafkaError) {
      admin.describe_configs(resources).wait.resources
    }
  end

  def test_describe_configs_multiple_existing_topics
    admin.create_topic(topic_name, 2, 1).wait
    sleep(1)

    resources = [
      { resource_type: 2, resource_name: TestTopics.example_topic },
      { resource_type: 2, resource_name: topic_name }
    ]
    resources_results = admin.describe_configs(resources).wait.resources

    assert_equal 2, resources_results.size
    assert_equal 2, resources_results.first.type
    assert_equal TestTopics.example_topic, resources_results.first.name
    assert_equal 2, resources_results.last.type
    assert_equal topic_name, resources_results.last.name
  end

  def test_describe_configs_invalid_resource_type
    admin.create_topic(topic_name, 2, 1).wait
    sleep(1)

    resources = [{ resource_type: 0, resource_name: SecureRandom.uuid }]
    assert_raises(Rdkafka::RdkafkaError) {
      admin.describe_configs(resources).wait.resources
    }
  end

  def test_describe_configs_invalid_broker
    admin.create_topic(topic_name, 2, 1).wait
    sleep(1)

    resources = [{ resource_type: 4, resource_name: "non-existing" }]
    assert_raises(Rdkafka::RdkafkaError) {
      admin.describe_configs(resources).wait.resources
    }
  end

  def test_describe_configs_valid_broker
    admin.create_topic(topic_name, 2, 1).wait
    sleep(1)

    resources = [{ resource_type: 4, resource_name: "1" }]
    resources_results = admin.describe_configs(resources).wait.resources

    assert_equal 1, resources_results.size
    assert_equal 4, resources_results.first.type
    assert_equal "1", resources_results.first.name
    assert_operator resources_results.first.configs.size, :>, 230
    assert_equal "log.cleaner.min.compaction.lag.ms", resources_results.first.configs.first.name
    assert_equal "0", resources_results.first.configs.first.value
    refute_empty resources_results.first.configs.map(&:synonyms)
  end

  def test_describe_configs_valid_broker_with_topics
    admin.create_topic(topic_name, 2, 1).wait
    sleep(1)

    resources = [
      { resource_type: 4, resource_name: "1" },
      { resource_type: 2, resource_name: topic_name }
    ]
    resources_results = admin.describe_configs(resources).wait.resources

    assert_equal 2, resources_results.size
    assert_equal 4, resources_results.first.type
    assert_equal "1", resources_results.first.name
    assert_operator resources_results.first.configs.size, :>, 230
    assert_equal "log.cleaner.min.compaction.lag.ms", resources_results.first.configs.first.name
    assert_equal "0", resources_results.first.configs.first.value
    assert_equal 2, resources_results.last.type
    assert_equal topic_name, resources_results.last.name
    assert_operator resources_results.last.configs.size, :>, 25
    assert_equal "compression.type", resources_results.last.configs.first.name
    assert_equal "producer", resources_results.last.configs.first.value
  end

  # incremental_alter_configs tests

  def test_incremental_alter_configs_set_valid_config
    admin.create_topic(topic_name, 2, 1).wait
    sleep(1)

    target_retention = rand(86400002..86410001).to_s
    resources_with_configs = [
      {
        resource_type: 2,
        resource_name: topic_name,
        configs: [
          {
            name: "delete.retention.ms",
            value: target_retention,
            op_type: 0
          }
        ]
      }
    ]
    resources_results = admin.incremental_alter_configs(resources_with_configs).wait.resources

    assert_equal 1, resources_results.size
    assert_equal 2, resources_results.first.type
    assert_equal topic_name, resources_results.first.name

    sleep(1)

    ret_config = admin.describe_configs(resources_with_configs).wait.resources.first.configs.find do |c|
      c.name == "delete.retention.ms"
    end

    assert_equal target_retention, ret_config.value
  end

  def test_incremental_alter_configs_delete_valid_config
    admin.create_topic(topic_name, 2, 1).wait
    sleep(1)

    target_retention = rand(8640002..8650001).to_s
    resources_with_configs = [
      {
        resource_type: 2,
        resource_name: topic_name,
        configs: [
          {
            name: "delete.retention.ms",
            value: target_retention,
            op_type: 1
          }
        ]
      }
    ]
    resources_results = admin.incremental_alter_configs(resources_with_configs).wait.resources

    assert_equal 1, resources_results.size
    assert_equal 2, resources_results.first.type
    assert_equal topic_name, resources_results.first.name

    sleep(1)

    ret_config = admin.describe_configs(resources_with_configs).wait.resources.first.configs.find do |c|
      c.name == "delete.retention.ms"
    end

    assert_equal "86400000", ret_config.value
  end

  def test_incremental_alter_configs_append_valid_config
    admin.create_topic(topic_name, 2, 1).wait
    sleep(1)

    target_policy = "compact"
    resources_with_configs = [
      {
        resource_type: 2,
        resource_name: topic_name,
        configs: [
          {
            name: "cleanup.policy",
            value: target_policy,
            op_type: 2
          }
        ]
      }
    ]
    resources_results = admin.incremental_alter_configs(resources_with_configs).wait.resources

    assert_equal 1, resources_results.size
    assert_equal 2, resources_results.first.type
    assert_equal topic_name, resources_results.first.name

    sleep(1)

    ret_config = admin.describe_configs(resources_with_configs).wait.resources.first.configs.find do |c|
      c.name == "cleanup.policy"
    end

    assert_equal "delete,#{target_policy}", ret_config.value
  end

  def test_incremental_alter_configs_subtract_valid_config
    admin.create_topic(topic_name, 2, 1).wait
    sleep(1)

    target_policy = "delete"
    resources_with_configs = [
      {
        resource_type: 2,
        resource_name: topic_name,
        configs: [
          {
            name: "cleanup.policy",
            value: target_policy,
            op_type: 3
          }
        ]
      }
    ]
    resources_results = admin.incremental_alter_configs(resources_with_configs).wait.resources

    assert_equal 1, resources_results.size
    assert_equal 2, resources_results.first.type
    assert_equal topic_name, resources_results.first.name

    sleep(1)

    ret_config = admin.describe_configs(resources_with_configs).wait.resources.first.configs.find do |c|
      c.name == "cleanup.policy"
    end

    assert_equal "", ret_config.value
  end

  def test_incremental_alter_configs_invalid_config
    admin.create_topic(topic_name, 2, 1).wait
    sleep(1)

    target_retention = "-10"
    resources_with_configs = [
      {
        resource_type: 2,
        resource_name: topic_name,
        configs: [
          {
            name: "delete.retention.ms",
            value: target_retention,
            op_type: 0
          }
        ]
      }
    ]
    error = assert_raises(Rdkafka::RdkafkaError) {
      admin.incremental_alter_configs(resources_with_configs).wait.resources
    }
    assert_match(/invalid_config/, error.message)
  end

  # list_offsets tests

  def test_list_offsets_returns_earliest_offsets
    topic = TestTopics.consume_test_topic

    report = admin.list_offsets(
      { topic => [{ partition: 0, offset: :earliest }] }
    ).wait(max_wait_timeout_ms: 15_000)

    assert_kind_of Rdkafka::Admin::ListOffsetsReport, report
    assert_operator report.offsets.length, :>=, 1

    first = report.offsets.first

    assert_equal topic, first[:topic]
    assert_equal 0, first[:partition]
    assert_operator first[:offset], :>=, 0
  end

  def test_list_offsets_returns_latest_offsets
    topic = TestTopics.consume_test_topic

    report = admin.list_offsets(
      { topic => [{ partition: 0, offset: :latest }] }
    ).wait(max_wait_timeout_ms: 15_000)

    assert_operator report.offsets.length, :>=, 1

    first = report.offsets.first

    assert_equal topic, first[:topic]
    assert_equal 0, first[:partition]
    assert_operator first[:offset], :>=, 0
  end

  def test_list_offsets_returns_offsets_for_multiple_partitions
    topic = TestTopics.consume_test_topic

    report = admin.list_offsets(
      { topic => [
        { partition: 0, offset: :earliest },
        { partition: 1, offset: :latest }
      ] }
    ).wait(max_wait_timeout_ms: 15_000)

    assert_equal 2, report.offsets.length
    assert_equal [0, 1], report.offsets.map { |o| o[:partition] }.sort
  end

  def test_list_offsets_with_read_committed_isolation_level
    topic = TestTopics.consume_test_topic

    report = admin.list_offsets(
      { topic => [{ partition: 0, offset: :latest }] },
      isolation_level: Rdkafka::Bindings::RD_KAFKA_ISOLATION_LEVEL_READ_COMMITTED
    ).wait(max_wait_timeout_ms: 15_000)

    assert_equal 1, report.offsets.length
  end

  def test_list_offsets_by_timestamp
    topic = TestTopics.consume_test_topic

    # Use a timestamp of 0 (epoch) to get earliest messages
    report = admin.list_offsets(
      { topic => [{ partition: 0, offset: 0 }] }
    ).wait(max_wait_timeout_ms: 15_000)

    assert_equal 1, report.offsets.length
    first = report.offsets.first

    assert_equal topic, first[:topic]
    assert_equal 0, first[:partition]
  end

  def test_list_offsets_when_admin_is_closed
    admin.close
    assert_raises(Rdkafka::ClosedAdminError) {
      admin.list_offsets({ "topic" => [{ partition: 0, offset: :earliest }] })
    }
  end

  def test_list_offsets_edge_case_null_background_queue
    Rdkafka::Bindings.stub(:rd_kafka_queue_get_background, FFI::Pointer::NULL) do
      error = assert_raises(Rdkafka::Config::ConfigError) {
        admin.list_offsets({ "topic" => [{ partition: 0, offset: :earliest }] })
      }
      assert_match(/rd_kafka_queue_get_background was NULL/, error.message)
    end
  end

  def test_list_offsets_edge_case_list_offsets_raises
    Rdkafka::Bindings.stub(:rd_kafka_ListOffsets, proc { raise "oops" }) do
      error = assert_raises(RuntimeError) {
        admin.list_offsets({ "topic" => [{ partition: 0, offset: :earliest }] })
      }
      assert_match(/oops/, error.message)
    end
  end

  def test_list_offsets_with_unknown_symbol_offset
    error = assert_raises(ArgumentError) {
      admin.list_offsets({ "topic" => [{ partition: 0, offset: :unknown }] })
    }
    assert_match(/Unknown offset specification/, error.message)
  end

  # delete_topic tests

  def test_delete_topic_with_invalid_topic_name
    delete_topic_handle = admin.delete_topic("[!@#]")
    ex = assert_raises(Rdkafka::RdkafkaError) {
      delete_topic_handle.wait(max_wait_timeout_ms: 15_000)
    }
    assert_kind_of Rdkafka::RdkafkaError, ex
    assert_match(/Broker: Unknown topic or partition \(unknown_topic_or_part\)/, ex.message)
    assert_match(/Broker: Unknown topic or partition/, ex.broker_message)
  end

  def test_delete_topic_with_non_existing_topic
    delete_topic_handle = admin.delete_topic(topic_name)
    ex = assert_raises(Rdkafka::RdkafkaError) {
      delete_topic_handle.wait(max_wait_timeout_ms: 15_000)
    }
    assert_kind_of Rdkafka::RdkafkaError, ex
    assert_match(/Broker: Unknown topic or partition \(unknown_topic_or_part\)/, ex.message)
    assert_match(/Broker: Unknown topic or partition/, ex.broker_message)
  end

  def test_delete_topic_edge_case_null_background_queue
    Rdkafka::Bindings.stub(:rd_kafka_queue_get_background, FFI::Pointer::NULL) do
      error = assert_raises(Rdkafka::Config::ConfigError) {
        admin.delete_topic(topic_name)
      }
      assert_match(/rd_kafka_queue_get_background was NULL/, error.message)
    end
  end

  def test_delete_topic_edge_case_delete_topics_raises
    Rdkafka::Bindings.stub(:rd_kafka_DeleteTopics, proc { raise "oops" }) do
      error = assert_raises(RuntimeError) {
        admin.delete_topic(topic_name)
      }
      assert_match(/oops/, error.message)
    end
  end

  def test_delete_topic_deletes_newly_created_topic
    create_topic_handle = admin.create_topic(topic_name, topic_partition_count, topic_replication_factor)
    create_topic_report = create_topic_handle.wait(max_wait_timeout_ms: 15_000)

    assert_nil create_topic_report.error_string
    assert_equal topic_name, create_topic_report.result_name

    # Retry topic deletion a few times. On CI Kafka seems to not
    # always be ready for it immediately
    delete_topic_report = nil
    10.times do |i|
      delete_topic_handle = admin.delete_topic(topic_name)
      delete_topic_report = delete_topic_handle.wait(max_wait_timeout_ms: 15_000)
      break
    rescue Rdkafka::RdkafkaError => ex
      if i > 3
        raise ex
      end
    end

    assert_nil delete_topic_report.error_string
    assert_equal topic_name, delete_topic_report.result_name
  end

  # ACL tests for topic resource

  def test_acl_create_acl_for_non_existing_topic
    # Setup: create topic for testing acl
    create_topic_handle = admin.create_topic(resource_name, topic_partition_count, topic_replication_factor)
    create_topic_handle.wait(max_wait_timeout_ms: 15_000)

    non_existing_resource_name = "non-existing-topic"

    # acl creation for resources that does not exist will still get created successfully.
    create_acl_handle = admin.create_acl(resource_type: resource_type, resource_name: non_existing_resource_name, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
    create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, create_acl_report.rdkafka_response
    assert_equal "", create_acl_report.rdkafka_response_string

    # delete the acl that was created for a non existing topic
    delete_acl_handle = admin.delete_acl(resource_type: resource_type, resource_name: non_existing_resource_name, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
    delete_acl_report = delete_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, delete_acl_handle[:response]
    assert_equal 1, delete_acl_report.deleted_acls.size
  ensure
    # Cleanup
    begin
      admin.delete_acl(resource_type: resource_type, resource_name: resource_name, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
    begin
      admin.delete_topic(resource_name).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
  end

  def test_acl_create_acl_for_newly_created_topic
    # Setup: create topic for testing acl
    create_topic_handle = admin.create_topic(resource_name, topic_partition_count, topic_replication_factor)
    create_topic_handle.wait(max_wait_timeout_ms: 15_000)

    create_acl_handle = admin.create_acl(resource_type: resource_type, resource_name: resource_name, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
    create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, create_acl_report.rdkafka_response
    assert_equal "", create_acl_report.rdkafka_response_string
  ensure
    # Cleanup
    begin
      admin.delete_acl(resource_type: resource_type, resource_name: resource_name, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
    begin
      admin.delete_topic(resource_name).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
  end

  def test_acl_describe_acl_for_non_existing_topic
    # Setup: create topic for testing acl
    create_topic_handle = admin.create_topic(resource_name, topic_partition_count, topic_replication_factor)
    create_topic_handle.wait(max_wait_timeout_ms: 15_000)

    non_existing_resource_name = "non-existing-topic"

    describe_acl_handle = admin.describe_acl(resource_type: resource_type, resource_name: non_existing_resource_name, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
    describe_acl_report = describe_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, describe_acl_handle[:response]
    assert_equal 0, describe_acl_report.acls.size
  ensure
    # Cleanup
    begin
      admin.delete_acl(resource_type: resource_type, resource_name: resource_name, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
    begin
      admin.delete_topic(resource_name).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
  end

  def test_acl_create_and_describe_newly_created_acls
    # Setup: create topic for testing acl
    create_topic_handle = admin.create_topic(resource_name, topic_partition_count, topic_replication_factor)
    create_topic_handle.wait(max_wait_timeout_ms: 15_000)

    # create_acl
    create_acl_handle = admin.create_acl(resource_type: resource_type, resource_name: "test_acl_topic_1", resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
    create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, create_acl_report.rdkafka_response
    assert_equal "", create_acl_report.rdkafka_response_string

    create_acl_handle = admin.create_acl(resource_type: resource_type, resource_name: "test_acl_topic_2", resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
    create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, create_acl_report.rdkafka_response
    assert_equal "", create_acl_report.rdkafka_response_string

    # Since we create and immediately check, this is slow on loaded CIs, hence we wait
    sleep(2)

    # describe_acl
    describe_acl_handle = admin.describe_acl(resource_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_ANY, resource_name: nil, resource_pattern_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_ANY, principal: nil, host: nil, operation: Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_ANY, permission_type: Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ANY)
    describe_acl_report = describe_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, describe_acl_handle[:response]
    assert_equal 2, describe_acl_report.acls.length
  ensure
    # Cleanup
    begin
      admin.delete_acl(resource_type: resource_type, resource_name: resource_name, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
    begin
      admin.delete_topic(resource_name).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
  end

  def test_acl_delete_acl_for_non_existing_topic
    # Setup: create topic for testing acl
    create_topic_handle = admin.create_topic(resource_name, topic_partition_count, topic_replication_factor)
    create_topic_handle.wait(max_wait_timeout_ms: 15_000)

    non_existing_resource_name = "non-existing-topic"

    delete_acl_handle = admin.delete_acl(resource_type: resource_type, resource_name: non_existing_resource_name, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
    delete_acl_report = delete_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, delete_acl_handle[:response]
    assert_equal 0, delete_acl_report.deleted_acls.size
  ensure
    # Cleanup
    begin
      admin.delete_acl(resource_type: resource_type, resource_name: resource_name, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
    begin
      admin.delete_topic(resource_name).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
  end

  def test_acl_create_and_delete_newly_created_acls
    # Setup: create topic for testing acl
    create_topic_handle = admin.create_topic(resource_name, topic_partition_count, topic_replication_factor)
    create_topic_handle.wait(max_wait_timeout_ms: 15_000)

    # create_acl
    create_acl_handle = admin.create_acl(resource_type: resource_type, resource_name: "test_acl_topic_1", resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
    create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, create_acl_report.rdkafka_response
    assert_equal "", create_acl_report.rdkafka_response_string

    create_acl_handle = admin.create_acl(resource_type: resource_type, resource_name: "test_acl_topic_2", resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
    create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, create_acl_report.rdkafka_response
    assert_equal "", create_acl_report.rdkafka_response_string

    # delete_acl - resource_name nil - to delete all acls with any resource name and matching all other filters.
    delete_acl_handle = admin.delete_acl(resource_type: resource_type, resource_name: nil, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
    delete_acl_report = delete_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, delete_acl_handle[:response]
    assert_equal 2, delete_acl_report.deleted_acls.length
  ensure
    # Cleanup
    begin
      admin.delete_acl(resource_type: resource_type, resource_name: resource_name, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
    begin
      admin.delete_topic(resource_name).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
  end

  # ACL tests for transactional_id

  def test_transactional_id_acl_create
    transactional_id_resource_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_TRANSACTIONAL_ID
    transactional_id_resource_pattern_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL
    transactional_id_principal = "User:test-user"
    transactional_id_host = "*"
    transactional_id_operation = Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_WRITE
    transactional_id_permission_type = Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW
    transactional_id_resource_name = "test-transactional-id"

    create_acl_handle = admin.create_acl(
      resource_type: transactional_id_resource_type,
      resource_name: transactional_id_resource_name,
      resource_pattern_type: transactional_id_resource_pattern_type,
      principal: transactional_id_principal,
      host: transactional_id_host,
      operation: transactional_id_operation,
      permission_type: transactional_id_permission_type
    )
    create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, create_acl_report.rdkafka_response
    assert_equal "", create_acl_report.rdkafka_response_string
  ensure
    begin
      admin.delete_acl(
        resource_type: transactional_id_resource_type,
        resource_name: nil,
        resource_pattern_type: transactional_id_resource_pattern_type,
        principal: transactional_id_principal,
        host: transactional_id_host,
        operation: transactional_id_operation,
        permission_type: transactional_id_permission_type
      ).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
  end

  def test_transactional_id_acl_create_for_non_existing
    transactional_id_resource_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_TRANSACTIONAL_ID
    transactional_id_resource_pattern_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL
    transactional_id_principal = "User:test-user"
    transactional_id_host = "*"
    transactional_id_operation = Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_WRITE
    transactional_id_permission_type = Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW
    non_existing_transactional_id = "non-existing-transactional-id"

    # ACL creation for transactional_ids that don't exist will still get created successfully
    create_acl_handle = admin.create_acl(
      resource_type: transactional_id_resource_type,
      resource_name: non_existing_transactional_id,
      resource_pattern_type: transactional_id_resource_pattern_type,
      principal: transactional_id_principal,
      host: transactional_id_host,
      operation: transactional_id_operation,
      permission_type: transactional_id_permission_type
    )
    create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, create_acl_report.rdkafka_response
    assert_equal "", create_acl_report.rdkafka_response_string

    # Clean up the ACL that was created for the non-existing transactional_id
    delete_acl_handle = admin.delete_acl(
      resource_type: transactional_id_resource_type,
      resource_name: non_existing_transactional_id,
      resource_pattern_type: transactional_id_resource_pattern_type,
      principal: transactional_id_principal,
      host: transactional_id_host,
      operation: transactional_id_operation,
      permission_type: transactional_id_permission_type
    )
    delete_acl_report = delete_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, delete_acl_handle[:response]
    assert_equal 1, delete_acl_report.deleted_acls.size
  ensure
    begin
      admin.delete_acl(
        resource_type: transactional_id_resource_type,
        resource_name: nil,
        resource_pattern_type: transactional_id_resource_pattern_type,
        principal: transactional_id_principal,
        host: transactional_id_host,
        operation: transactional_id_operation,
        permission_type: transactional_id_permission_type
      ).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
  end

  def test_transactional_id_acl_describe_non_existing
    transactional_id_resource_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_TRANSACTIONAL_ID
    transactional_id_resource_pattern_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL
    transactional_id_principal = "User:test-user"
    transactional_id_host = "*"
    transactional_id_operation = Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_WRITE
    transactional_id_permission_type = Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW
    non_existing_transactional_id = "non-existing-transactional-id"

    describe_acl_handle = admin.describe_acl(
      resource_type: transactional_id_resource_type,
      resource_name: non_existing_transactional_id,
      resource_pattern_type: transactional_id_resource_pattern_type,
      principal: transactional_id_principal,
      host: transactional_id_host,
      operation: transactional_id_operation,
      permission_type: transactional_id_permission_type
    )
    describe_acl_report = describe_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, describe_acl_handle[:response]
    assert_equal 0, describe_acl_report.acls.size
  ensure
    begin
      admin.delete_acl(
        resource_type: transactional_id_resource_type,
        resource_name: nil,
        resource_pattern_type: transactional_id_resource_pattern_type,
        principal: transactional_id_principal,
        host: transactional_id_host,
        operation: transactional_id_operation,
        permission_type: transactional_id_permission_type
      ).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
  end

  def test_transactional_id_acl_create_and_describe
    transactional_id_resource_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_TRANSACTIONAL_ID
    transactional_id_resource_pattern_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL
    transactional_id_principal = "User:test-user"
    transactional_id_host = "*"
    transactional_id_operation = Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_WRITE
    transactional_id_permission_type = Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW

    # Create first ACL
    create_acl_handle = admin.create_acl(
      resource_type: transactional_id_resource_type,
      resource_name: "test_transactional_id_1",
      resource_pattern_type: transactional_id_resource_pattern_type,
      principal: transactional_id_principal,
      host: transactional_id_host,
      operation: transactional_id_operation,
      permission_type: transactional_id_permission_type
    )
    create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, create_acl_report.rdkafka_response
    assert_equal "", create_acl_report.rdkafka_response_string

    # Create second ACL
    create_acl_handle = admin.create_acl(
      resource_type: transactional_id_resource_type,
      resource_name: "test_transactional_id_2",
      resource_pattern_type: transactional_id_resource_pattern_type,
      principal: transactional_id_principal,
      host: transactional_id_host,
      operation: transactional_id_operation,
      permission_type: transactional_id_permission_type
    )
    create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, create_acl_report.rdkafka_response
    assert_equal "", create_acl_report.rdkafka_response_string

    # Since we create and immediately check, this is slow on loaded CIs, hence we wait
    sleep(2)

    # Describe ACLs - filter by transactional_id resource type
    describe_acl_handle = admin.describe_acl(
      resource_type: transactional_id_resource_type,
      resource_name: nil,
      resource_pattern_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_ANY,
      principal: transactional_id_principal,
      host: transactional_id_host,
      operation: transactional_id_operation,
      permission_type: transactional_id_permission_type
    )
    describe_acl_report = describe_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, describe_acl_handle[:response]
    assert_equal 2, describe_acl_report.acls.length
  ensure
    begin
      admin.delete_acl(
        resource_type: transactional_id_resource_type,
        resource_name: nil,
        resource_pattern_type: transactional_id_resource_pattern_type,
        principal: transactional_id_principal,
        host: transactional_id_host,
        operation: transactional_id_operation,
        permission_type: transactional_id_permission_type
      ).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
  end

  def test_transactional_id_acl_delete_non_existing
    transactional_id_resource_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_TRANSACTIONAL_ID
    transactional_id_resource_pattern_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL
    transactional_id_principal = "User:test-user"
    transactional_id_host = "*"
    transactional_id_operation = Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_WRITE
    transactional_id_permission_type = Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW
    non_existing_transactional_id = "non-existing-transactional-id"

    delete_acl_handle = admin.delete_acl(
      resource_type: transactional_id_resource_type,
      resource_name: non_existing_transactional_id,
      resource_pattern_type: transactional_id_resource_pattern_type,
      principal: transactional_id_principal,
      host: transactional_id_host,
      operation: transactional_id_operation,
      permission_type: transactional_id_permission_type
    )
    delete_acl_report = delete_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, delete_acl_handle[:response]
    assert_equal 0, delete_acl_report.deleted_acls.size
  ensure
    begin
      admin.delete_acl(
        resource_type: transactional_id_resource_type,
        resource_name: nil,
        resource_pattern_type: transactional_id_resource_pattern_type,
        principal: transactional_id_principal,
        host: transactional_id_host,
        operation: transactional_id_operation,
        permission_type: transactional_id_permission_type
      ).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
  end

  def test_transactional_id_acl_create_and_delete
    transactional_id_resource_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_TRANSACTIONAL_ID
    transactional_id_resource_pattern_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL
    transactional_id_principal = "User:test-user"
    transactional_id_host = "*"
    transactional_id_operation = Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_WRITE
    transactional_id_permission_type = Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW

    # Create first ACL
    create_acl_handle = admin.create_acl(
      resource_type: transactional_id_resource_type,
      resource_name: "test_transactional_id_1",
      resource_pattern_type: transactional_id_resource_pattern_type,
      principal: transactional_id_principal,
      host: transactional_id_host,
      operation: transactional_id_operation,
      permission_type: transactional_id_permission_type
    )
    create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, create_acl_report.rdkafka_response
    assert_equal "", create_acl_report.rdkafka_response_string

    # Create second ACL
    create_acl_handle = admin.create_acl(
      resource_type: transactional_id_resource_type,
      resource_name: "test_transactional_id_2",
      resource_pattern_type: transactional_id_resource_pattern_type,
      principal: transactional_id_principal,
      host: transactional_id_host,
      operation: transactional_id_operation,
      permission_type: transactional_id_permission_type
    )
    create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, create_acl_report.rdkafka_response
    assert_equal "", create_acl_report.rdkafka_response_string

    # Delete ACLs - resource_name nil to delete all ACLs with any resource name and matching all other filters
    delete_acl_handle = admin.delete_acl(
      resource_type: transactional_id_resource_type,
      resource_name: nil,
      resource_pattern_type: transactional_id_resource_pattern_type,
      principal: transactional_id_principal,
      host: transactional_id_host,
      operation: transactional_id_operation,
      permission_type: transactional_id_permission_type
    )
    delete_acl_report = delete_acl_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal 0, delete_acl_handle[:response]
    assert_equal 2, delete_acl_report.deleted_acls.length
  ensure
    begin
      admin.delete_acl(
        resource_type: transactional_id_resource_type,
        resource_name: nil,
        resource_pattern_type: transactional_id_resource_pattern_type,
        principal: transactional_id_principal,
        host: transactional_id_host,
        operation: transactional_id_operation,
        permission_type: transactional_id_permission_type
      ).wait(max_wait_timeout_ms: 15_000)
    rescue
      nil
    end
  end

  # Group tests

  def test_delete_group_with_existing_group
    consumer_config = rdkafka_consumer_config("group.id": group_name)
    producer_config = rdkafka_producer_config
    producer = producer_config.producer
    consumer = consumer_config.consumer

    # Create a topic, post a message to it, consume it and commit offsets, this will create a group that we can then delete.
    admin.create_topic(topic_name, topic_partition_count, topic_replication_factor).wait(max_wait_timeout_ms: 15_000)

    producer.produce(topic: topic_name, payload: "test", key: "test").wait(max_wait_timeout_ms: 15_000)

    consumer.subscribe(topic_name)
    wait_for_assignment(consumer)
    message = nil

    10.times do
      message ||= consumer.poll(100)
    end

    refute_nil message

    consumer.commit
    consumer.close

    delete_group_handle = admin.delete_group(group_name)
    report = delete_group_handle.wait(max_wait_timeout_ms: 15_000)

    assert_equal group_name, report.result_name
  ensure
    producer&.close
    consumer&.close
  end

  def test_delete_group_with_non_existing_group
    delete_group_handle = admin.delete_group(group_name)

    ex = assert_raises(Rdkafka::RdkafkaError) {
      delete_group_handle.wait(max_wait_timeout_ms: 15_000)
    }
    assert_kind_of Rdkafka::RdkafkaError, ex
    assert_match(/Broker: The group id does not exist \(group_id_not_found\)/, ex.message)
  end

  # create_partitions tests

  def test_create_partitions_topic_does_not_exist
    error = assert_raises(Rdkafka::RdkafkaError) {
      admin.create_partitions(topic_name, 10).wait
    }
    assert_match(/unknown_topic_or_part/, error.message)
  end

  def test_create_partitions_topic_already_has_desired_count
    admin.create_topic(topic_name, 2, 1).wait

    error = assert_raises(Rdkafka::RdkafkaError) {
      admin.create_partitions(topic_name, 2).wait
    }
    assert_match(/invalid_partitions/, error.message)

    metadata = begin
      admin.metadata(topic_name).topics.first
    rescue Rdkafka::RdkafkaError
      sleep(1)
      admin.metadata(topic_name).topics.first
    end

    assert_equal 2, metadata[:partition_count]
  end

  def test_create_partitions_topic_has_more_than_requested
    admin.create_topic(topic_name, 5, 1).wait

    error = assert_raises(Rdkafka::RdkafkaError) {
      admin.create_partitions(topic_name, 2).wait
    }
    assert_match(/invalid_partitions/, error.message)

    # On slow CI this may propagate, thus we wait a bit
    sleep(1)

    metadata = begin
      admin.metadata(topic_name).topics.first
    rescue Rdkafka::RdkafkaError
      sleep(1)
      admin.metadata(topic_name).topics.first
    end

    assert_equal 5, metadata[:partition_count]
  end

  def test_create_partitions_topic_has_less_than_desired
    admin.create_topic(topic_name, 1, 1).wait
    sleep(1)

    admin.create_partitions(topic_name, 10).wait
    sleep(1)

    metadata = begin
      admin.metadata(topic_name).topics.first
    rescue Rdkafka::RdkafkaError
      sleep(1)
      admin.metadata(topic_name).topics.first
    end

    assert_equal 10, metadata[:partition_count]
  end

  # oauthbearer_set_token tests

  def test_oauthbearer_set_token_when_sasl_not_configured
    response = admin.oauthbearer_set_token(
      token: "foo",
      lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
      principal_name: "kafka-cluster"
    )

    assert_equal Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE, response
  end

  def test_oauthbearer_set_token_when_sasl_configured_without_extensions
    config_sasl = rdkafka_config(
      "security.protocol": "sasl_ssl",
      "sasl.mechanisms": "OAUTHBEARER"
    )
    admin_sasl = config_sasl.admin

    response = admin_sasl.oauthbearer_set_token(
      token: "foo",
      lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
      principal_name: "kafka-cluster"
    )

    assert_equal 0, response
  ensure
    admin_sasl&.close
  end

  def test_oauthbearer_set_token_when_sasl_configured_with_extensions
    config_sasl = rdkafka_config(
      "security.protocol": "sasl_ssl",
      "sasl.mechanisms": "OAUTHBEARER"
    )
    admin_sasl = config_sasl.admin

    response = admin_sasl.oauthbearer_set_token(
      token: "foo",
      lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
      principal_name: "kafka-cluster",
      extensions: {
        "foo" => "bar"
      }
    )

    assert_equal 0, response
  ensure
    admin_sasl&.close
  end

  # events_poll_nb_each tests

  def test_events_poll_nb_each_does_not_raise_when_queue_is_empty
    admin.events_poll_nb_each { |_| }
  end

  def test_events_poll_nb_each_yields_count_after_each_poll
    counts = []
    call_count = 0
    poll_proc = proc {
      call_count += 1
      (call_count <= 2) ? 1 : 0
    }

    Rdkafka::Bindings.stub(:rd_kafka_poll_nb, poll_proc) do
      admin.events_poll_nb_each { |count| counts << count }
    end

    assert_equal [1, 1], counts
  end

  def test_events_poll_nb_each_stops_when_block_returns_stop
    iterations = 0

    Rdkafka::Bindings.stub(:rd_kafka_poll_nb, 1) do
      admin.events_poll_nb_each do |_count|
        iterations += 1
        :stop if iterations >= 3
      end
    end

    assert_equal 3, iterations
  end

  def test_events_poll_nb_each_raises_when_admin_is_closed
    admin.close
    error = assert_raises(Rdkafka::ClosedAdminError) {
      admin.events_poll_nb_each { |_| }
    }
    assert_match(/events_poll_nb_each/, error.message)
  end

  # file descriptor access for fiber scheduler integration tests

  def test_enable_queue_io_events_on_admin_queue
    admin_no_poll = config.admin(run_polling_thread: false)
    signal_r, signal_w = IO.pipe
    admin_no_poll.enable_queue_io_events(signal_w.fileno)
    signal_r.close
    signal_w.close
  ensure
    admin_no_poll&.close
  end

  def test_enable_background_queue_io_events
    admin_no_poll = config.admin(run_polling_thread: false)
    signal_r, signal_w = IO.pipe
    admin_no_poll.enable_background_queue_io_events(signal_w.fileno)
    signal_r.close
    signal_w.close
  ensure
    admin_no_poll&.close
  end

  def test_enable_queue_io_events_raises_when_admin_is_closed
    admin_no_poll = config.admin(run_polling_thread: false)
    admin_no_poll.close
    signal_r, signal_w = IO.pipe
    assert_raises(Rdkafka::ClosedInnerError) {
      admin_no_poll.enable_queue_io_events(signal_w.fileno)
    }
    signal_r.close
    signal_w.close
  end

  def test_enable_background_queue_io_events_raises_when_admin_is_closed
    admin_no_poll = config.admin(run_polling_thread: false)
    admin_no_poll.close
    signal_r, signal_w = IO.pipe
    assert_raises(Rdkafka::ClosedInnerError) {
      admin_no_poll.enable_background_queue_io_events(signal_w.fileno)
    }
    signal_r.close
    signal_w.close
  end

  # fork tests

  unless RUBY_PLATFORM == "java"
    def test_operating_from_a_fork_can_create_topics
      # If the FFI issue is not mitigated, this will hang forever
      pid = fork do
        admin
          .create_topic(topic_name, topic_partition_count, topic_replication_factor)
          .wait
      end

      Process.wait(pid)
    end
  end
end
