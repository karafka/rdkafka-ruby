# frozen_string_literal: true

require "ostruct"

describe Rdkafka::Admin do
  let(:config) { rdkafka_config }
  let(:topic_name) { "test-topic-#{SecureRandom.uuid}" }
  let(:topic_partition_count) { 3 }
  let(:topic_replication_factor) { 1 }
  let(:topic_config) { { "cleanup.policy" => "compact", "min.cleanable.dirty.ratio" => 0.8 } }
  let(:invalid_topic_config) { { "cleeeeenup.policee" => "campact" } }
  let(:group_name) { "test-group-#{SecureRandom.uuid}" }
  let(:resource_name) { TestTopics.unique }
  let(:resource_type) { Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC }
  let(:resource_pattern_type) { Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL }
  let(:principal) { "User:anonymous" }
  let(:host) { "*" }
  let(:operation) { Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ }
  let(:permission_type) { Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW }
  let(:admin) { config.admin }

  after do
    # Registry should always end up being empty
    assert_empty Rdkafka::Admin::CreateTopicHandle::REGISTRY
    assert_empty Rdkafka::Admin::CreatePartitionsHandle::REGISTRY
    assert_empty Rdkafka::Admin::DescribeAclHandle::REGISTRY
    assert_empty Rdkafka::Admin::CreateAclHandle::REGISTRY
    assert_empty Rdkafka::Admin::DeleteAclHandle::REGISTRY
    assert_empty Rdkafka::Admin::ListOffsetsHandle::REGISTRY
    admin&.close
  end

  describe "#describe_errors" do
    it "returns 172 errors" do
      errors = Rdkafka::Admin.describe_errors

      assert_equal 172, errors.size
    end

    it "includes negative error codes" do
      errors = Rdkafka::Admin.describe_errors

      assert_equal({ code: -184, description: "Local: Queue full", name: "_QUEUE_FULL" }, errors[-184])
    end

    it "includes positive error codes" do
      errors = Rdkafka::Admin.describe_errors

      assert_equal({ code: 21, description: "Broker: Invalid required acks value", name: "INVALID_REQUIRED_ACKS" }, errors[21])
    end
  end

  describe "admin without auto-start" do
    let(:admin) { config.admin(native_kafka_auto_start: false) }

    it "can start and close" do
      admin.start
      admin.close
    end

    it "can close without starting" do
      admin.close
    end
  end

  describe "#create_topic" do
    describe "called with invalid input" do
      it "raises an exception with an invalid topic name" do
        create_topic_handle = admin.create_topic("[!@#]", topic_partition_count, topic_replication_factor)
        ex = assert_raises(Rdkafka::RdkafkaError) {
          create_topic_handle.wait(max_wait_timeout_ms: 15_000)
        }
        assert_kind_of Rdkafka::RdkafkaError, ex
        assert_match(/Broker: Invalid topic \(topic_exception\)/, ex.message)
        assert_match(/Topic name.*is invalid: .* contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'/, ex.broker_message)
      end

      it "raises an exception with the name of a topic that already exists" do
        existing_topic_name = TestTopics.empty_test_topic
        create_topic_handle = admin.create_topic(existing_topic_name, topic_partition_count, topic_replication_factor)
        ex = assert_raises(Rdkafka::RdkafkaError) {
          create_topic_handle.wait(max_wait_timeout_ms: 15_000)
        }
        assert_kind_of Rdkafka::RdkafkaError, ex
        assert_match(/Broker: Topic already exists \(topic_already_exists\)/, ex.message)
        assert_match(/Topic '#{Regexp.escape(TestTopics.empty_test_topic)}' already exists/, ex.broker_message)
      end

      it "raises an exception with an invalid partition count" do
        error = assert_raises(Rdkafka::Config::ConfigError) {
          admin.create_topic(topic_name, -999, topic_replication_factor)
        }
        assert_match(/num_partitions out of expected range/, error.message)
      end

      it "raises an exception with an invalid replication factor" do
        error = assert_raises(Rdkafka::Config::ConfigError) {
          admin.create_topic(topic_name, topic_partition_count, -2)
        }
        assert_match(/replication_factor out of expected range/, error.message)
      end

      it "raises an exception with an invalid topic configuration" do
        create_topic_handle = admin.create_topic(topic_name, topic_partition_count, topic_replication_factor, invalid_topic_config)
        error = assert_raises(Rdkafka::RdkafkaError) {
          create_topic_handle.wait(max_wait_timeout_ms: 15_000)
        }
        assert_match(/Broker: Configuration is invalid \(invalid_config\)/, error.message)
      end
    end

    describe "edge cases" do
      it "raises when background queue is NULL" do
        Rdkafka::Bindings.stub(:rd_kafka_queue_get_background, FFI::Pointer::NULL) do
          error = assert_raises(Rdkafka::Config::ConfigError) {
            admin.create_topic(topic_name, topic_partition_count, topic_replication_factor)
          }
          assert_match(/rd_kafka_queue_get_background was NULL/, error.message)
        end
      end

      it "raises when CreateTopics raises" do
        Rdkafka::Bindings.stub(:rd_kafka_CreateTopics, proc { raise "oops" }) do
          error = assert_raises(RuntimeError) {
            admin.create_topic(topic_name, topic_partition_count, topic_replication_factor)
          }
          assert_match(/oops/, error.message)
        end
      end
    end

    it "creates a topic" do
      create_topic_handle = admin.create_topic(topic_name, topic_partition_count, topic_replication_factor, topic_config)
      create_topic_report = create_topic_handle.wait(max_wait_timeout_ms: 15_000)

      assert_nil create_topic_report.error_string
      assert_equal topic_name, create_topic_report.result_name
    end
  end

  describe "#describe_configs" do
    it "describes an existing topic" do
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

    it "raises for a non-existing topic" do
      admin.create_topic(topic_name, 2, 1).wait
      sleep(1)

      resources = [{ resource_type: 2, resource_name: SecureRandom.uuid }]
      error = assert_raises(Rdkafka::RdkafkaError) {
        admin.describe_configs(resources).wait.resources
      }
      assert_match(/unknown_topic_or_part/, error.message)
    end

    it "raises when mixing existing and non-existing topics" do
      admin.create_topic(topic_name, 2, 1).wait
      sleep(1)

      resources = [
        { resource_type: 2, resource_name: topic_name },
        { resource_type: 2, resource_name: SecureRandom.uuid }
      ]
      error = assert_raises(Rdkafka::RdkafkaError) {
        admin.describe_configs(resources).wait.resources
      }
      assert_match(/unknown_topic_or_part/, error.message)
    end

    it "describes multiple existing topics" do
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

    it "raises for an invalid resource type" do
      admin.create_topic(topic_name, 2, 1).wait
      sleep(1)

      resources = [{ resource_type: 0, resource_name: SecureRandom.uuid }]
      error = assert_raises(Rdkafka::RdkafkaError) {
        admin.describe_configs(resources).wait.resources
      }
      assert_match(/invalid_request/, error.message)
    end

    it "raises for an invalid broker" do
      admin.create_topic(topic_name, 2, 1).wait
      sleep(1)

      resources = [{ resource_type: 4, resource_name: "non-existing" }]
      error = assert_raises(Rdkafka::RdkafkaError) {
        admin.describe_configs(resources).wait.resources
      }
      assert_match(/invalid_arg/, error.message)
    end

    it "describes a valid broker" do
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

    it "describes a valid broker with topics" do
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
  end

  describe "#incremental_alter_configs" do
    it "sets a valid config" do
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

    it "deletes a valid config" do
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

    it "appends a valid config" do
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

    it "subtracts a valid config" do
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

    it "raises for an invalid config" do
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
  end

  describe "#list_offsets" do
    it "returns earliest offsets" do
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

    it "returns latest offsets" do
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

    it "returns offsets for multiple partitions" do
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

    it "works with read_committed isolation level" do
      topic = TestTopics.consume_test_topic

      report = admin.list_offsets(
        { topic => [{ partition: 0, offset: :latest }] },
        isolation_level: Rdkafka::Bindings::RD_KAFKA_ISOLATION_LEVEL_READ_COMMITTED
      ).wait(max_wait_timeout_ms: 15_000)

      assert_equal 1, report.offsets.length
    end

    it "returns offsets by timestamp" do
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

    it "raises when admin is closed" do
      admin.close
      assert_raises(Rdkafka::ClosedAdminError) {
        admin.list_offsets({ "topic" => [{ partition: 0, offset: :earliest }] })
      }
    end

    describe "edge cases" do
      it "raises when background queue is NULL" do
        Rdkafka::Bindings.stub(:rd_kafka_queue_get_background, FFI::Pointer::NULL) do
          error = assert_raises(Rdkafka::Config::ConfigError) {
            admin.list_offsets({ "topic" => [{ partition: 0, offset: :earliest }] })
          }
          assert_match(/rd_kafka_queue_get_background was NULL/, error.message)
        end
      end

      it "raises when ListOffsets raises" do
        Rdkafka::Bindings.stub(:rd_kafka_ListOffsets, proc { raise "oops" }) do
          error = assert_raises(RuntimeError) {
            admin.list_offsets({ "topic" => [{ partition: 0, offset: :earliest }] })
          }
          assert_match(/oops/, error.message)
        end
      end

      it "raises with unknown symbol offset" do
        error = assert_raises(ArgumentError) {
          admin.list_offsets({ "topic" => [{ partition: 0, offset: :unknown }] })
        }
        assert_match(/Unknown offset specification/, error.message)
      end
    end
  end

  describe "#delete_topic" do
    describe "called with invalid input" do
      it "raises an exception with an invalid topic name" do
        delete_topic_handle = admin.delete_topic("[!@#]")
        ex = assert_raises(Rdkafka::RdkafkaError) {
          delete_topic_handle.wait(max_wait_timeout_ms: 15_000)
        }
        assert_kind_of Rdkafka::RdkafkaError, ex
        assert_match(/Broker: Unknown topic or partition \(unknown_topic_or_part\)/, ex.message)
        assert_match(/Broker: Unknown topic or partition/, ex.broker_message)
      end

      it "raises an exception with a non-existing topic" do
        delete_topic_handle = admin.delete_topic(topic_name)
        ex = assert_raises(Rdkafka::RdkafkaError) {
          delete_topic_handle.wait(max_wait_timeout_ms: 15_000)
        }
        assert_kind_of Rdkafka::RdkafkaError, ex
        assert_match(/Broker: Unknown topic or partition \(unknown_topic_or_part\)/, ex.message)
        assert_match(/Broker: Unknown topic or partition/, ex.broker_message)
      end
    end

    describe "edge cases" do
      it "raises when background queue is NULL" do
        Rdkafka::Bindings.stub(:rd_kafka_queue_get_background, FFI::Pointer::NULL) do
          error = assert_raises(Rdkafka::Config::ConfigError) {
            admin.delete_topic(topic_name)
          }
          assert_match(/rd_kafka_queue_get_background was NULL/, error.message)
        end
      end

      it "raises when DeleteTopics raises" do
        Rdkafka::Bindings.stub(:rd_kafka_DeleteTopics, proc { raise "oops" }) do
          error = assert_raises(RuntimeError) {
            admin.delete_topic(topic_name)
          }
          assert_match(/oops/, error.message)
        end
      end
    end

    it "deletes a newly created topic" do
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
  end

  describe "ACL operations" do
    describe "for topic resource" do
      it "creates an ACL for a non-existing topic" do
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

      it "creates an ACL for a newly created topic" do
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

      it "describes an ACL for a non-existing topic" do
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

      it "creates and describes newly created ACLs" do
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

      it "deletes an ACL for a non-existing topic" do
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

      it "creates and deletes newly created ACLs" do
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
    end

    describe "for transactional_id" do
      it "creates an ACL" do
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

      it "creates an ACL for a non-existing transactional_id" do
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

      it "describes an ACL for a non-existing transactional_id" do
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

      it "creates and describes ACLs" do
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

      it "deletes an ACL for a non-existing transactional_id" do
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

      it "creates and deletes ACLs" do
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
    end
  end

  describe "#delete_group" do
    it "deletes an existing group" do
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

    it "raises for a non-existing group" do
      # Warm up the broker's group coordinator by briefly joining a consumer
      # group. In RSpec, the "existing group" test always ran first (defined
      # order) which did this implicitly.
      warmup_consumer = rdkafka_consumer_config.consumer
      warmup_consumer.subscribe(TestTopics.consume_test_topic)
      wait_for_assignment(warmup_consumer)
      warmup_consumer.close

      delete_group_handle = admin.delete_group(group_name)

      ex = assert_raises(Rdkafka::RdkafkaError) {
        delete_group_handle.wait(max_wait_timeout_ms: 15_000)
      }
      assert_kind_of Rdkafka::RdkafkaError, ex
      assert_match(/group_id_not_found/, ex.message)
    end
  end

  describe "#create_partitions" do
    it "raises when topic does not exist" do
      error = assert_raises(Rdkafka::RdkafkaError) {
        admin.create_partitions(topic_name, 10).wait
      }
      assert_match(/unknown_topic_or_part/, error.message)
    end

    it "raises when topic already has the desired partition count" do
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

    it "raises when topic has more partitions than requested" do
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

    it "increases partition count when topic has fewer than desired" do
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
  end

  describe "#oauthbearer_set_token" do
    it "returns error when SASL is not configured" do
      response = admin.oauthbearer_set_token(
        token: "foo",
        lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
        principal_name: "kafka-cluster"
      )

      assert_equal Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE, response
    end

    it "succeeds when SASL is configured without extensions" do
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

    it "succeeds when SASL is configured with extensions" do
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
  end

  describe "#events_poll_nb_each" do
    it "does not raise when queue is empty" do
      admin.events_poll_nb_each { |_| }
    end

    it "yields count after each poll" do
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

    it "stops when block returns :stop" do
      iterations = 0

      Rdkafka::Bindings.stub(:rd_kafka_poll_nb, 1) do
        admin.events_poll_nb_each do |_count|
          iterations += 1
          :stop if iterations >= 3
        end
      end

      assert_equal 3, iterations
    end

    it "raises when admin is closed" do
      admin.close
      error = assert_raises(Rdkafka::ClosedAdminError) {
        admin.events_poll_nb_each { |_| }
      }
      assert_match(/events_poll_nb_each/, error.message)
    end
  end

  describe "file descriptor access for fiber scheduler integration" do
    it "enables queue IO events on admin queue" do
      admin_no_poll = config.admin(run_polling_thread: false)
      signal_r, signal_w = IO.pipe
      admin_no_poll.enable_queue_io_events(signal_w.fileno)
      signal_r.close
      signal_w.close
    ensure
      admin_no_poll&.close
    end

    it "enables background queue IO events" do
      admin_no_poll = config.admin(run_polling_thread: false)
      signal_r, signal_w = IO.pipe
      admin_no_poll.enable_background_queue_io_events(signal_w.fileno)
      signal_r.close
      signal_w.close
    ensure
      admin_no_poll&.close
    end

    it "raises when enabling queue IO events on a closed admin" do
      admin_no_poll = config.admin(run_polling_thread: false)
      admin_no_poll.close
      signal_r, signal_w = IO.pipe
      assert_raises(Rdkafka::ClosedInnerError) {
        admin_no_poll.enable_queue_io_events(signal_w.fileno)
      }
      signal_r.close
      signal_w.close
    end

    it "raises when enabling background queue IO events on a closed admin" do
      admin_no_poll = config.admin(run_polling_thread: false)
      admin_no_poll.close
      signal_r, signal_w = IO.pipe
      assert_raises(Rdkafka::ClosedInnerError) {
        admin_no_poll.enable_background_queue_io_events(signal_w.fileno)
      }
      signal_r.close
      signal_w.close
    end
  end

  unless RUBY_PLATFORM == "java"
    describe "fork support" do
      it "can create topics from a fork" do
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
end
