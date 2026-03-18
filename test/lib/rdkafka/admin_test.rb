# frozen_string_literal: true

require "ostruct"

require_relative "../../test_helper"

describe Rdkafka::Admin do
  before do
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

  def admin
    @admin ||= @config.admin
  end

  after do
    # Registry should always end up being empty
    assert_empty Rdkafka::Admin::CreateTopicHandle::REGISTRY
    assert_empty Rdkafka::Admin::CreatePartitionsHandle::REGISTRY
    assert_empty Rdkafka::Admin::DescribeAclHandle::REGISTRY
    assert_empty Rdkafka::Admin::CreateAclHandle::REGISTRY
    assert_empty Rdkafka::Admin::DeleteAclHandle::REGISTRY
    assert_empty Rdkafka::Admin::ListOffsetsHandle::REGISTRY
    admin.close
  end

  describe "#describe_errors" do
    it "has 172 errors" do
      errors = admin.class.describe_errors
      assert_equal 172, errors.size
    end

    it "has the correct error for code -184" do
      errors = admin.class.describe_errors
      assert_equal({ code: -184, description: "Local: Queue full", name: "_QUEUE_FULL" }, errors[-184])
    end

    it "has the correct error for code 21" do
      errors = admin.class.describe_errors
      assert_equal({ code: 21, description: "Broker: Invalid required acks value", name: "INVALID_REQUIRED_ACKS" }, errors[21])
    end
  end

  describe "admin without auto-start" do
    it "expect to be able to start it later and close" do
      @admin = @config.admin(native_kafka_auto_start: false)
      admin.start
      admin.close
    end

    it "expect to be able to close it without starting" do
      @admin = @config.admin(native_kafka_auto_start: false)
      admin.close
    end
  end

  describe "#create_topic" do
    describe "called with invalid input" do
      describe "with an invalid topic name" do
        it "raises an exception" do
          topic_name = "[!@#]"
          create_topic_handle = admin.create_topic(topic_name, @topic_partition_count, @topic_replication_factor)
          ex = assert_raises(Rdkafka::RdkafkaError) do
            create_topic_handle.wait(max_wait_timeout_ms: 15_000)
          end
          assert_kind_of Rdkafka::RdkafkaError, ex
          assert_match(/Broker: Invalid topic \(topic_exception\)/, ex.message)
          assert_match(/Topic name.*is invalid: .* contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'/, ex.broker_message)
        end
      end

      describe "with the name of a topic that already exists" do
        it "raises an exception" do
          existing_topic_name = TestTopics.create
          create_topic_handle = admin.create_topic(existing_topic_name, @topic_partition_count, @topic_replication_factor)
          ex = assert_raises(Rdkafka::RdkafkaError) do
            create_topic_handle.wait(max_wait_timeout_ms: 15_000)
          end
          assert_kind_of Rdkafka::RdkafkaError, ex
          assert_match(/Broker: Topic already exists \(topic_already_exists\)/, ex.message)
          assert_match(/Topic '#{Regexp.escape(existing_topic_name)}' already exists/, ex.broker_message)
        end
      end

      describe "with an invalid partition count" do
        it "raises an exception" do
          e = assert_raises(Rdkafka::Config::ConfigError) do
            admin.create_topic(@topic_name, -999, @topic_replication_factor)
          end
          assert_match(/num_partitions out of expected range/, e.message)
        end
      end

      describe "with an invalid replication factor" do
        it "raises an exception" do
          e = assert_raises(Rdkafka::Config::ConfigError) do
            admin.create_topic(@topic_name, @topic_partition_count, -2)
          end
          assert_match(/replication_factor out of expected range/, e.message)
        end
      end

      describe "with an invalid topic configuration" do
        it "doesn't create the topic" do
          create_topic_handle = admin.create_topic(@topic_name, @topic_partition_count, @topic_replication_factor, @invalid_topic_config)
          e = assert_raises(Rdkafka::RdkafkaError) do
            create_topic_handle.wait(max_wait_timeout_ms: 15_000)
          end
          assert_match(/Broker: Configuration is invalid \(invalid_config\)/, e.message)
        end
      end
    end

    context "edge case" do
      context "where we are unable to get the background queue" do
        before do
          Rdkafka::Bindings.stubs(:rd_kafka_queue_get_background).returns(FFI::Pointer::NULL)
        end

        it "raises an exception" do
          e = assert_raises(Rdkafka::Config::ConfigError) do
            admin.create_topic(@topic_name, @topic_partition_count, @topic_replication_factor)
          end
          assert_match(/rd_kafka_queue_get_background was NULL/, e.message)
        end
      end

      context "where rd_kafka_CreateTopics raises an exception" do
        before do
          Rdkafka::Bindings.stubs(:rd_kafka_CreateTopics).raises(RuntimeError.new("oops"))
        end

        it "raises an exception" do
          e = assert_raises(RuntimeError) do
            admin.create_topic(@topic_name, @topic_partition_count, @topic_replication_factor)
          end
          assert_match(/oops/, e.message)
        end
      end
    end

    it "creates a topic" do
      create_topic_handle = admin.create_topic(@topic_name, @topic_partition_count, @topic_replication_factor, @topic_config)
      create_topic_report = create_topic_handle.wait(max_wait_timeout_ms: 15_000)
      assert_nil create_topic_report.error_string
      assert_equal @topic_name, create_topic_report.result_name
    end
  end

  describe "describe_configs" do
    before do
      admin.create_topic(@topic_name, 2, 1).wait
      sleep(1)
    end

    context "when describing config of an existing topic" do
      it "returns the config" do
        resources = [{ resource_type: 2, resource_name: @topic_name }]
        resources_results = admin.describe_configs(resources).wait.resources
        assert_equal 1, resources_results.size
        assert_equal 2, resources_results.first.type
        assert_equal @topic_name, resources_results.first.name
        assert_operator resources_results.first.configs.size, :>, 25
        assert_equal "compression.type", resources_results.first.configs.first.name
        assert_equal "producer", resources_results.first.configs.first.value
        refute_empty resources_results.first.configs.map(&:synonyms)
      end
    end

    context "when describing config of a non-existing topic" do
      it "expect to raise error" do
        resources = [{ resource_type: 2, resource_name: SecureRandom.uuid }]
        assert_raises(Rdkafka::RdkafkaError) do
          admin.describe_configs(resources).wait.resources
        end
      end
    end

    context "when describing both existing and non-existing topics" do
      it "expect to raise error" do
        resources = [
          { resource_type: 2, resource_name: @topic_name },
          { resource_type: 2, resource_name: SecureRandom.uuid }
        ]
        assert_raises(Rdkafka::RdkafkaError) do
          admin.describe_configs(resources).wait.resources
        end
      end
    end

    context "when describing multiple existing topics" do
      it "returns configs for both" do
        resources = [
          { resource_type: 2, resource_name: TestTopics.example_topic },
          { resource_type: 2, resource_name: @topic_name }
        ]
        resources_results = admin.describe_configs(resources).wait.resources
        assert_equal 2, resources_results.size
        assert_equal 2, resources_results.first.type
        assert_equal TestTopics.example_topic, resources_results.first.name
        assert_equal 2, resources_results.last.type
        assert_equal @topic_name, resources_results.last.name
      end
    end

    context "when trying to describe invalid resource type" do
      it "expect to raise error" do
        resources = [{ resource_type: 0, resource_name: SecureRandom.uuid }]
        assert_raises(Rdkafka::RdkafkaError) do
          admin.describe_configs(resources).wait.resources
        end
      end
    end

    context "when trying to describe invalid broker" do
      it "expect to raise error" do
        resources = [{ resource_type: 4, resource_name: "non-existing" }]
        assert_raises(Rdkafka::RdkafkaError) do
          admin.describe_configs(resources).wait.resources
        end
      end
    end

    context "when trying to describe valid broker" do
      it "returns broker config" do
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
    end

    context "when describing valid broker with topics in one request" do
      it "returns both broker and topic configs" do
        resources = [
          { resource_type: 4, resource_name: "1" },
          { resource_type: 2, resource_name: @topic_name }
        ]
        resources_results = admin.describe_configs(resources).wait.resources
        assert_equal 2, resources_results.size
        assert_equal 4, resources_results.first.type
        assert_equal "1", resources_results.first.name
        assert_operator resources_results.first.configs.size, :>, 230
        assert_equal "log.cleaner.min.compaction.lag.ms", resources_results.first.configs.first.name
        assert_equal "0", resources_results.first.configs.first.value
        assert_equal 2, resources_results.last.type
        assert_equal @topic_name, resources_results.last.name
        assert_operator resources_results.last.configs.size, :>, 25
        assert_equal "compression.type", resources_results.last.configs.first.name
        assert_equal "producer", resources_results.last.configs.first.value
      end
    end
  end

  describe "incremental_alter_configs" do
    before do
      admin.create_topic(@topic_name, 2, 1).wait
      sleep(1)
    end

    context "when altering one topic with one valid config via set" do
      it "alters the config" do
        target_retention = rand(86400002..86410001).to_s
        resources_with_configs = [
          {
            resource_type: 2,
            resource_name: @topic_name,
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
        assert_equal @topic_name, resources_results.first.name

        sleep(1)

        ret_config = admin.describe_configs(resources_with_configs).wait.resources.first.configs.find do |config|
          config.name == "delete.retention.ms"
        end

        assert_equal target_retention, ret_config.value
      end
    end

    context "when altering one topic with one valid config via delete" do
      it "resets the config to default" do
        target_retention = rand(8640002..8650001).to_s
        resources_with_configs = [
          {
            resource_type: 2,
            resource_name: @topic_name,
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
        assert_equal @topic_name, resources_results.first.name

        sleep(1)

        ret_config = admin.describe_configs(resources_with_configs).wait.resources.first.configs.find do |config|
          config.name == "delete.retention.ms"
        end

        assert_equal "86400000", ret_config.value
      end
    end

    context "when altering one topic with one valid config via append" do
      it "appends to the config" do
        target_policy = "compact"
        resources_with_configs = [
          {
            resource_type: 2,
            resource_name: @topic_name,
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
        assert_equal @topic_name, resources_results.first.name

        sleep(1)

        ret_config = admin.describe_configs(resources_with_configs).wait.resources.first.configs.find do |config|
          config.name == "cleanup.policy"
        end

        assert_equal "delete,#{target_policy}", ret_config.value
      end
    end

    context "when altering one topic with one valid config via subtrack" do
      it "subtracts from the config" do
        target_policy = "delete"
        resources_with_configs = [
          {
            resource_type: 2,
            resource_name: @topic_name,
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
        assert_equal @topic_name, resources_results.first.name

        sleep(1)

        ret_config = admin.describe_configs(resources_with_configs).wait.resources.first.configs.find do |config|
          config.name == "cleanup.policy"
        end

        assert_equal "", ret_config.value
      end
    end

    context "when altering one topic with invalid config" do
      it "expect to raise error" do
        target_retention = "-10"
        resources_with_configs = [
          {
            resource_type: 2,
            resource_name: @topic_name,
            configs: [
              {
                name: "delete.retention.ms",
                value: target_retention,
                op_type: 0
              }
            ]
          }
        ]
        e = assert_raises(Rdkafka::RdkafkaError) do
          admin.incremental_alter_configs(resources_with_configs).wait.resources
        end
        assert_match(/invalid_config/, e.message)
      end
    end
  end

  describe "#list_offsets" do
    context "when querying offsets for an existing topic with messages" do
      before do
        @lo_topic = TestTopics.create
        # Produce a message to ensure partition leaders are fully established
        lo_producer = rdkafka_config.producer
        lo_producer.produce(topic: @lo_topic, payload: "warmup", partition: 0).wait
        lo_producer.close
      end

      it "returns earliest offsets" do
        report = admin.list_offsets(
          { @lo_topic => [{ partition: 0, offset: :earliest }] }
        ).wait(max_wait_timeout_ms: 15_000)

        assert_kind_of Rdkafka::Admin::ListOffsetsReport, report
        assert_operator report.offsets.length, :>=, 1

        first = report.offsets.first
        assert_equal @lo_topic, first[:topic]
        assert_equal 0, first[:partition]
        assert_operator first[:offset], :>=, 0
      end

      it "returns latest offsets" do
        report = admin.list_offsets(
          { @lo_topic => [{ partition: 0, offset: :latest }] }
        ).wait(max_wait_timeout_ms: 15_000)

        assert_operator report.offsets.length, :>=, 1

        first = report.offsets.first
        assert_equal @lo_topic, first[:topic]
        assert_equal 0, first[:partition]
        assert_operator first[:offset], :>=, 0
      end

      it "returns offsets for multiple partitions at once" do
        report = admin.list_offsets(
          { @lo_topic => [
            { partition: 0, offset: :earliest },
            { partition: 1, offset: :latest }
          ] }
        ).wait(max_wait_timeout_ms: 15_000)

        assert_equal 2, report.offsets.length
        assert_equal [0, 1], report.offsets.map { |o| o[:partition] }.sort
      end

      it "returns offsets with read_committed isolation level" do
        report = admin.list_offsets(
          { @lo_topic => [{ partition: 0, offset: :latest }] },
          isolation_level: Rdkafka::Bindings::RD_KAFKA_ISOLATION_LEVEL_READ_COMMITTED
        ).wait(max_wait_timeout_ms: 15_000)

        assert_equal 1, report.offsets.length
      end
    end

    context "when querying offsets by timestamp" do
      it "returns offsets for a given timestamp" do
        lo_topic = TestTopics.create
        # Use a timestamp of 0 (epoch) to get earliest messages.
        # Retry on transient broker errors (not_leader_for_partition) that can
        # occur when partition leadership hasn't fully settled after topic creation.
        report = nil
        3.times do
          report = admin.list_offsets(
            { lo_topic => [{ partition: 0, offset: 0 }] }
          ).wait(max_wait_timeout_ms: 15_000)
          break
        rescue Rdkafka::RdkafkaError => e
          raise unless e.message.include?("not_leader_for_partition")

          sleep(1)
        end

        assert_equal 1, report.offsets.length
        first = report.offsets.first
        assert_equal lo_topic, first[:topic]
        assert_equal 0, first[:partition]
      end
    end

    context "when admin is closed" do
      it "raises ClosedAdminError" do
        admin.close
        assert_raises(Rdkafka::ClosedAdminError) do
          admin.list_offsets({ "topic" => [{ partition: 0, offset: :earliest }] })
        end
      end
    end

    context "edge case" do
      context "where we are unable to get the background queue" do
        before do
          Rdkafka::Bindings.stubs(:rd_kafka_queue_get_background).returns(FFI::Pointer::NULL)
        end

        it "raises an exception" do
          e = assert_raises(Rdkafka::Config::ConfigError) do
            admin.list_offsets({ "topic" => [{ partition: 0, offset: :earliest }] })
          end
          assert_match(/rd_kafka_queue_get_background was NULL/, e.message)
        end
      end

      context "where rd_kafka_ListOffsets raises an exception" do
        before do
          Rdkafka::Bindings.stubs(:rd_kafka_ListOffsets).raises(RuntimeError.new("oops"))
        end

        it "raises an exception" do
          e = assert_raises(RuntimeError) do
            admin.list_offsets({ "topic" => [{ partition: 0, offset: :earliest }] })
          end
          assert_match(/oops/, e.message)
        end
      end
    end

    context "with invalid offset specification" do
      it "raises ArgumentError for unknown symbol" do
        e = assert_raises(ArgumentError) do
          admin.list_offsets({ "topic" => [{ partition: 0, offset: :unknown }] })
        end
        assert_match(/Unknown offset specification/, e.message)
      end
    end
  end

  describe "#delete_topic" do
    describe "called with invalid input" do
      describe "with an invalid topic name" do
        it "raises an exception" do
          topic_name = "[!@#]"
          delete_topic_handle = admin.delete_topic(topic_name)
          ex = assert_raises(Rdkafka::RdkafkaError) do
            delete_topic_handle.wait(max_wait_timeout_ms: 15_000)
          end
          assert_kind_of Rdkafka::RdkafkaError, ex
          assert_match(/Broker: Unknown topic or partition \(unknown_topic_or_part\)/, ex.message)
          assert_match(/Broker: Unknown topic or partition/, ex.broker_message)
        end
      end

      describe "with the name of a topic that does not exist" do
        it "raises an exception" do
          delete_topic_handle = admin.delete_topic(@topic_name)
          ex = assert_raises(Rdkafka::RdkafkaError) do
            delete_topic_handle.wait(max_wait_timeout_ms: 15_000)
          end
          assert_kind_of Rdkafka::RdkafkaError, ex
          assert_match(/Broker: Unknown topic or partition \(unknown_topic_or_part\)/, ex.message)
          assert_match(/Broker: Unknown topic or partition/, ex.broker_message)
        end
      end
    end

    context "edge case" do
      context "where we are unable to get the background queue" do
        before do
          Rdkafka::Bindings.stubs(:rd_kafka_queue_get_background).returns(FFI::Pointer::NULL)
        end

        it "raises an exception" do
          e = assert_raises(Rdkafka::Config::ConfigError) do
            admin.delete_topic(@topic_name)
          end
          assert_match(/rd_kafka_queue_get_background was NULL/, e.message)
        end
      end

      context "where rd_kafka_DeleteTopics raises an exception" do
        before do
          Rdkafka::Bindings.stubs(:rd_kafka_DeleteTopics).raises(RuntimeError.new("oops"))
        end

        it "raises an exception" do
          e = assert_raises(RuntimeError) do
            admin.delete_topic(@topic_name)
          end
          assert_match(/oops/, e.message)
        end
      end
    end

    it "deletes a topic that was newly created" do
      create_topic_handle = admin.create_topic(@topic_name, @topic_partition_count, @topic_replication_factor)
      create_topic_report = create_topic_handle.wait(max_wait_timeout_ms: 15_000)
      assert_nil create_topic_report.error_string
      assert_equal @topic_name, create_topic_report.result_name

      # Retry topic deletion a few times. On CI Kafka seems to not
      # always be ready for it immediately
      delete_topic_report = nil
      10.times do |i|
        delete_topic_handle = admin.delete_topic(@topic_name)
        delete_topic_report = delete_topic_handle.wait(max_wait_timeout_ms: 15_000)
        break
      rescue Rdkafka::RdkafkaError => ex
        if i > 3
          raise ex
        end
      end

      assert_nil delete_topic_report.error_string
      assert_equal @topic_name, delete_topic_report.result_name
    end
  end

  describe "#ACL tests for topic resource" do
    before do
      @non_existing_resource_name = "non-existing-topic"
      # create topic for testing acl
      create_topic_handle = admin.create_topic(@resource_name, @topic_partition_count, @topic_replication_factor)
      create_topic_handle.wait(max_wait_timeout_ms: 15_000)
    end

    after do
      # delete acl
      delete_acl_handle = admin.delete_acl(resource_type: @resource_type, resource_name: @resource_name, resource_pattern_type: @resource_pattern_type, principal: @principal, host: @host, operation: @operation, permission_type: @permission_type)
      delete_acl_handle.wait(max_wait_timeout_ms: 15_000)

      # delete topic that was created for testing acl
      delete_topic_handle = admin.delete_topic(@resource_name)
      delete_topic_handle.wait(max_wait_timeout_ms: 15_000)
    end

    describe "#create_acl" do
      it "create acl for a topic that does not exist" do
        # acl creation for resources that does not exist will still get created successfully.
        create_acl_handle = admin.create_acl(resource_type: @resource_type, resource_name: @non_existing_resource_name, resource_pattern_type: @resource_pattern_type, principal: @principal, host: @host, operation: @operation, permission_type: @permission_type)
        create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, create_acl_report.rdkafka_response
        assert_equal "", create_acl_report.rdkafka_response_string

        # delete the acl that was created for a non existing topic"
        delete_acl_handle = admin.delete_acl(resource_type: @resource_type, resource_name: @non_existing_resource_name, resource_pattern_type: @resource_pattern_type, principal: @principal, host: @host, operation: @operation, permission_type: @permission_type)
        delete_acl_report = delete_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, delete_acl_handle[:response]
        assert_equal 1, delete_acl_report.deleted_acls.size
      end

      it "creates a acl for topic that was newly created" do
        create_acl_handle = admin.create_acl(resource_type: @resource_type, resource_name: @resource_name, resource_pattern_type: @resource_pattern_type, principal: @principal, host: @host, operation: @operation, permission_type: @permission_type)
        create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, create_acl_report.rdkafka_response
        assert_equal "", create_acl_report.rdkafka_response_string
      end
    end

    describe "#describe_acl" do
      it "describe acl of a topic that does not exist" do
        describe_acl_handle = admin.describe_acl(resource_type: @resource_type, resource_name: @non_existing_resource_name, resource_pattern_type: @resource_pattern_type, principal: @principal, host: @host, operation: @operation, permission_type: @permission_type)
        describe_acl_report = describe_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, describe_acl_handle[:response]
        assert_equal 0, describe_acl_report.acls.size
      end

      it "create acls and describe the newly created acls" do
        # create_acl
        create_acl_handle = admin.create_acl(resource_type: @resource_type, resource_name: "test_acl_topic_1", resource_pattern_type: @resource_pattern_type, principal: @principal, host: @host, operation: @operation, permission_type: @permission_type)
        create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, create_acl_report.rdkafka_response
        assert_equal "", create_acl_report.rdkafka_response_string

        create_acl_handle = admin.create_acl(resource_type: @resource_type, resource_name: "test_acl_topic_2", resource_pattern_type: @resource_pattern_type, principal: @principal, host: @host, operation: @operation, permission_type: @permission_type)
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
      end
    end

    describe "#delete_acl" do
      it "delete acl of a topic that does not exist" do
        delete_acl_handle = admin.delete_acl(resource_type: @resource_type, resource_name: @non_existing_resource_name, resource_pattern_type: @resource_pattern_type, principal: @principal, host: @host, operation: @operation, permission_type: @permission_type)
        delete_acl_report = delete_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, delete_acl_handle[:response]
        assert_equal 0, delete_acl_report.deleted_acls.size
      end

      it "create an acl and delete the newly created acl" do
        # create_acl
        create_acl_handle = admin.create_acl(resource_type: @resource_type, resource_name: "test_acl_topic_1", resource_pattern_type: @resource_pattern_type, principal: @principal, host: @host, operation: @operation, permission_type: @permission_type)
        create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, create_acl_report.rdkafka_response
        assert_equal "", create_acl_report.rdkafka_response_string

        create_acl_handle = admin.create_acl(resource_type: @resource_type, resource_name: "test_acl_topic_2", resource_pattern_type: @resource_pattern_type, principal: @principal, host: @host, operation: @operation, permission_type: @permission_type)
        create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, create_acl_report.rdkafka_response
        assert_equal "", create_acl_report.rdkafka_response_string

        # delete_acl - resource_name nil - to delete all acls with any resource name and matching all other filters.
        delete_acl_handle = admin.delete_acl(resource_type: @resource_type, resource_name: nil, resource_pattern_type: @resource_pattern_type, principal: @principal, host: @host, operation: @operation, permission_type: @permission_type)
        delete_acl_report = delete_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, delete_acl_handle[:response]
        assert_equal 2, delete_acl_report.deleted_acls.length
      end
    end
  end

  describe "#ACL tests for transactional_id" do
    before do
      @transactional_id_resource_name = "test-transactional-id"
      @non_existing_transactional_id = "non-existing-transactional-id"
      @transactional_id_resource_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_TRANSACTIONAL_ID
      @transactional_id_resource_pattern_type = Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL
      @transactional_id_principal = "User:test-user"
      @transactional_id_host = "*"
      @transactional_id_operation = Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_WRITE
      @transactional_id_permission_type = Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW
    end

    after do
      # Clean up any ACLs that might have been created during tests
      delete_acl_handle = admin.delete_acl(
        resource_type: @transactional_id_resource_type,
        resource_name: nil,
        resource_pattern_type: @transactional_id_resource_pattern_type,
        principal: @transactional_id_principal,
        host: @transactional_id_host,
        operation: @transactional_id_operation,
        permission_type: @transactional_id_permission_type
      )
      delete_acl_handle.wait(max_wait_timeout_ms: 15_000)
    rescue
      # Ignore cleanup errors
    end

    describe "#create_acl" do
      it "creates acl for a transactional_id" do
        create_acl_handle = admin.create_acl(
          resource_type: @transactional_id_resource_type,
          resource_name: @transactional_id_resource_name,
          resource_pattern_type: @transactional_id_resource_pattern_type,
          principal: @transactional_id_principal,
          host: @transactional_id_host,
          operation: @transactional_id_operation,
          permission_type: @transactional_id_permission_type
        )
        create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, create_acl_report.rdkafka_response
        assert_equal "", create_acl_report.rdkafka_response_string
      end

      it "creates acl for a non-existing transactional_id" do
        # ACL creation for transactional_ids that don't exist will still get created successfully
        create_acl_handle = admin.create_acl(
          resource_type: @transactional_id_resource_type,
          resource_name: @non_existing_transactional_id,
          resource_pattern_type: @transactional_id_resource_pattern_type,
          principal: @transactional_id_principal,
          host: @transactional_id_host,
          operation: @transactional_id_operation,
          permission_type: @transactional_id_permission_type
        )
        create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, create_acl_report.rdkafka_response
        assert_equal "", create_acl_report.rdkafka_response_string

        # Clean up the ACL that was created for the non-existing transactional_id
        delete_acl_handle = admin.delete_acl(
          resource_type: @transactional_id_resource_type,
          resource_name: @non_existing_transactional_id,
          resource_pattern_type: @transactional_id_resource_pattern_type,
          principal: @transactional_id_principal,
          host: @transactional_id_host,
          operation: @transactional_id_operation,
          permission_type: @transactional_id_permission_type
        )
        delete_acl_report = delete_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, delete_acl_handle[:response]
        assert_equal 1, delete_acl_report.deleted_acls.size
      end
    end

    describe "#describe_acl" do
      it "describes acl of a transactional_id that does not exist" do
        describe_acl_handle = admin.describe_acl(
          resource_type: @transactional_id_resource_type,
          resource_name: @non_existing_transactional_id,
          resource_pattern_type: @transactional_id_resource_pattern_type,
          principal: @transactional_id_principal,
          host: @transactional_id_host,
          operation: @transactional_id_operation,
          permission_type: @transactional_id_permission_type
        )
        describe_acl_report = describe_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, describe_acl_handle[:response]
        assert_equal 0, describe_acl_report.acls.size
      end

      it "creates acls and describes the newly created transactional_id acls" do
        # Create first ACL
        create_acl_handle = admin.create_acl(
          resource_type: @transactional_id_resource_type,
          resource_name: "test_transactional_id_1",
          resource_pattern_type: @transactional_id_resource_pattern_type,
          principal: @transactional_id_principal,
          host: @transactional_id_host,
          operation: @transactional_id_operation,
          permission_type: @transactional_id_permission_type
        )
        create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, create_acl_report.rdkafka_response
        assert_equal "", create_acl_report.rdkafka_response_string

        # Create second ACL
        create_acl_handle = admin.create_acl(
          resource_type: @transactional_id_resource_type,
          resource_name: "test_transactional_id_2",
          resource_pattern_type: @transactional_id_resource_pattern_type,
          principal: @transactional_id_principal,
          host: @transactional_id_host,
          operation: @transactional_id_operation,
          permission_type: @transactional_id_permission_type
        )
        create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, create_acl_report.rdkafka_response
        assert_equal "", create_acl_report.rdkafka_response_string

        # Since we create and immediately check, this is slow on loaded CIs, hence we wait
        sleep(2)

        # Describe ACLs - filter by transactional_id resource type
        describe_acl_handle = admin.describe_acl(
          resource_type: @transactional_id_resource_type,
          resource_name: nil,
          resource_pattern_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_ANY,
          principal: @transactional_id_principal,
          host: @transactional_id_host,
          operation: @transactional_id_operation,
          permission_type: @transactional_id_permission_type
        )
        describe_acl_report = describe_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, describe_acl_handle[:response]
        assert_equal 2, describe_acl_report.acls.length
      end
    end

    describe "#delete_acl" do
      it "deletes acl of a transactional_id that does not exist" do
        delete_acl_handle = admin.delete_acl(
          resource_type: @transactional_id_resource_type,
          resource_name: @non_existing_transactional_id,
          resource_pattern_type: @transactional_id_resource_pattern_type,
          principal: @transactional_id_principal,
          host: @transactional_id_host,
          operation: @transactional_id_operation,
          permission_type: @transactional_id_permission_type
        )
        delete_acl_report = delete_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, delete_acl_handle[:response]
        assert_equal 0, delete_acl_report.deleted_acls.size
      end

      it "creates transactional_id acls and deletes the newly created acls" do
        # Create first ACL
        create_acl_handle = admin.create_acl(
          resource_type: @transactional_id_resource_type,
          resource_name: "test_transactional_id_1",
          resource_pattern_type: @transactional_id_resource_pattern_type,
          principal: @transactional_id_principal,
          host: @transactional_id_host,
          operation: @transactional_id_operation,
          permission_type: @transactional_id_permission_type
        )
        create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, create_acl_report.rdkafka_response
        assert_equal "", create_acl_report.rdkafka_response_string

        # Create second ACL
        create_acl_handle = admin.create_acl(
          resource_type: @transactional_id_resource_type,
          resource_name: "test_transactional_id_2",
          resource_pattern_type: @transactional_id_resource_pattern_type,
          principal: @transactional_id_principal,
          host: @transactional_id_host,
          operation: @transactional_id_operation,
          permission_type: @transactional_id_permission_type
        )
        create_acl_report = create_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, create_acl_report.rdkafka_response
        assert_equal "", create_acl_report.rdkafka_response_string

        # Delete ACLs - resource_name nil to delete all ACLs with any resource name and matching all other filters
        delete_acl_handle = admin.delete_acl(
          resource_type: @transactional_id_resource_type,
          resource_name: nil,
          resource_pattern_type: @transactional_id_resource_pattern_type,
          principal: @transactional_id_principal,
          host: @transactional_id_host,
          operation: @transactional_id_operation,
          permission_type: @transactional_id_permission_type
        )
        delete_acl_report = delete_acl_handle.wait(max_wait_timeout_ms: 15_000)
        assert_equal 0, delete_acl_handle[:response]
        assert_equal 2, delete_acl_report.deleted_acls.length
      end
    end
  end

  describe "Group tests" do
    describe "#delete_group" do
      describe "with an existing group" do
        before do
          @group_producer = rdkafka_producer_config.producer
          @group_consumer = rdkafka_consumer_config("group.id": @group_name).consumer

          # Create a topic, post a message to it, consume it and commit offsets, this will create a group that we can then delete.
          admin.create_topic(@topic_name, @topic_partition_count, @topic_replication_factor).wait(max_wait_timeout_ms: 15_000)

          @group_producer.produce(topic: @topic_name, payload: "test", key: "test").wait(max_wait_timeout_ms: 15_000)

          @group_consumer.subscribe(@topic_name)
          wait_for_assignment(@group_consumer)
          message = nil

          10.times do
            message ||= @group_consumer.poll(100)
          end

          refute_nil message

          @group_consumer.commit
          @group_consumer.close
        end

        after do
          @group_producer.close
          @group_consumer.close
        end

        it "deletes the group" do
          delete_group_handle = admin.delete_group(@group_name)
          report = delete_group_handle.wait(max_wait_timeout_ms: 15_000)

          assert_equal @group_name, report.result_name
        end
      end

      describe "called with invalid input" do
        describe "with the name of a group that does not exist" do
          it "raises an exception" do
            delete_group_handle = admin.delete_group(@group_name)

            ex = assert_raises(Rdkafka::RdkafkaError) do
              delete_group_handle.wait(max_wait_timeout_ms: 15_000)
            end
            assert_kind_of Rdkafka::RdkafkaError, ex
            assert_match(/group_id_not_found|not_coordinator/, ex.message)
          end
        end
      end
    end
  end

  describe "#create_partitions" do
    def metadata_for(topic_name)
      admin.metadata(topic_name).topics.first
    rescue Rdkafka::RdkafkaError
      # We have to wait because if we query too fast after topic creation request, it may not
      # yet be available throwing an error.
      # This occurs mostly on slow CIs
      sleep(1)
      admin.metadata(topic_name).topics.first
    end

    context "when topic does not exist" do
      it "expect to fail due to unknown partition" do
        e = assert_raises(Rdkafka::RdkafkaError) do
          admin.create_partitions(@topic_name, 10).wait
        end
        assert_match(/unknown_topic_or_part/, e.message)
      end
    end

    context "when topic already has the desired number of partitions" do
      before { admin.create_topic(@topic_name, 2, 1).wait }

      it "expect not to change number of partitions" do
        e = assert_raises(Rdkafka::RdkafkaError) do
          admin.create_partitions(@topic_name, 2).wait
        end
        assert_match(/invalid_partitions/, e.message)
        assert_equal 2, metadata_for(@topic_name)[:partition_count]
      end
    end

    context "when topic has more than the requested number of partitions" do
      before { admin.create_topic(@topic_name, 5, 1).wait }

      it "expect not to change number of partitions" do
        e = assert_raises(Rdkafka::RdkafkaError) do
          admin.create_partitions(@topic_name, 2).wait
        end
        assert_match(/invalid_partitions/, e.message)
        # On slow CI this may propagate, thus we wait a bit
        sleep(1)
        assert_equal 5, metadata_for(@topic_name)[:partition_count]
      end
    end

    context "when topic has less then desired number of partitions" do
      before do
        admin.create_topic(@topic_name, 1, 1).wait
        sleep(1)
      end

      it "expect to change number of partitions" do
        admin.create_partitions(@topic_name, 10).wait
        sleep(1)
        assert_equal 10, metadata_for(@topic_name)[:partition_count]
      end
    end
  end

  describe "#oauthbearer_set_token" do
    context "when sasl not configured" do
      it "returns RD_KAFKA_RESP_ERR__STATE" do
        response = admin.oauthbearer_set_token(
          token: "foo",
          lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
          principal_name: "kafka-cluster"
        )
        assert_equal Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE, response
      end
    end

    context "when sasl configured" do
      before do
        config_sasl = rdkafka_config(
          "security.protocol": "sasl_ssl",
          "sasl.mechanisms": "OAUTHBEARER"
        )
        @admin_sasl = config_sasl.admin
      end

      after do
        @admin_sasl.close
      end

      context "without extensions" do
        it "succeeds" do
          response = @admin_sasl.oauthbearer_set_token(
            token: "foo",
            lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
            principal_name: "kafka-cluster"
          )
          assert_equal 0, response
        end
      end

      context "with extensions" do
        it "succeeds" do
          response = @admin_sasl.oauthbearer_set_token(
            token: "foo",
            lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
            principal_name: "kafka-cluster",
            extensions: {
              "foo" => "bar"
            }
          )
          assert_equal 0, response
        end
      end
    end
  end

  describe "#events_poll_nb_each" do
    it "does not raise when queue is empty" do
      admin.events_poll_nb_each { |_| }
    end

    it "yields the count after each poll" do
      counts = []
      # Stub to return events, then zero
      call_count = 0
      Rdkafka::Bindings.stubs(:rd_kafka_poll_nb).with do
        call_count += 1
        true
      end.returns(1, 1, 0)

      admin.events_poll_nb_each { |count| counts << count }

      assert_equal [1, 1], counts
    end

    it "stops when block returns :stop" do
      iterations = 0
      # Stub to always return events
      Rdkafka::Bindings.stubs(:rd_kafka_poll_nb).returns(1)

      admin.events_poll_nb_each do |_count|
        iterations += 1
        :stop if iterations >= 3
      end

      assert_equal 3, iterations
    end

    context "when admin is closed" do
      before { admin.close }

      it "raises ClosedAdminError" do
        e = assert_raises(Rdkafka::ClosedAdminError) do
          admin.events_poll_nb_each { |_| }
        end
        assert_match(/events_poll_nb_each/, e.message)
      end
    end
  end

  describe "file descriptor access for fiber scheduler integration" do
    before do
      @admin = @config.admin(run_polling_thread: false)
    end

    it "enables IO events on admin queue" do
      signal_r, signal_w = IO.pipe
      admin.enable_queue_io_events(signal_w.fileno)
      signal_r.close
      signal_w.close
    end

    it "enables IO events on background queue" do
      signal_r, signal_w = IO.pipe
      admin.enable_background_queue_io_events(signal_w.fileno)
      signal_r.close
      signal_w.close
    end

    context "when admin is closed" do
      before { admin.close }

      it "raises ClosedInnerError when enabling queue_io_events" do
        signal_r, signal_w = IO.pipe
        assert_raises(Rdkafka::ClosedInnerError) do
          admin.enable_queue_io_events(signal_w.fileno)
        end
        signal_r.close
        signal_w.close
      end

      it "raises ClosedInnerError when enabling background_queue_io_events" do
        signal_r, signal_w = IO.pipe
        assert_raises(Rdkafka::ClosedInnerError) do
          admin.enable_background_queue_io_events(signal_w.fileno)
        end
        signal_r.close
        signal_w.close
      end
    end
  end

  unless RUBY_PLATFORM == "java"
    context "when operating from a fork" do
      # @see https://github.com/ffi/ffi/issues/1114
      it "expect to be able to create topics and run other admin operations without hanging" do
        # If the FFI issue is not mitigated, this will hang forever
        pid = fork do
          admin
            .create_topic(@topic_name, @topic_partition_count, @topic_replication_factor)
            .wait
        end

        Process.wait(pid)
      end
    end
  end
end
