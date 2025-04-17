# frozen_string_literal: true

require "ostruct"

describe Rdkafka::Admin do
  let(:config) { rdkafka_config }
  let(:admin)  { config.admin }

  after do
    # Registry should always end up being empty
    expect(Rdkafka::Admin::CreateTopicHandle::REGISTRY).to be_empty
    expect(Rdkafka::Admin::CreatePartitionsHandle::REGISTRY).to be_empty
    expect(Rdkafka::Admin::DescribeAclHandle::REGISTRY).to be_empty
    expect(Rdkafka::Admin::CreateAclHandle::REGISTRY).to be_empty
    expect(Rdkafka::Admin::DeleteAclHandle::REGISTRY).to be_empty
    admin.close
  end

  let(:topic_name)               { "test-topic-#{SecureRandom.uuid}" }
  let(:topic_partition_count)    { 3 }
  let(:topic_replication_factor) { 1 }
  let(:topic_config)             { {"cleanup.policy" => "compact", "min.cleanable.dirty.ratio" => 0.8} }
  let(:invalid_topic_config)     { {"cleeeeenup.policee" => "campact"} }
  let(:group_name)               { "test-group-#{SecureRandom.uuid}" }

  let(:resource_name)         {"acl-test-topic"}
  let(:resource_type)         {Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC}
  let(:resource_pattern_type) {Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL}
  let(:principal)             {"User:anonymous"}
  let(:host)                  {"*"}
  let(:operation)             {Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ}
  let(:permission_type)       {Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW}

  describe '#describe_errors' do
    let(:errors) { admin.class.describe_errors }

    it { expect(errors.size).to eq(170) }
    it { expect(errors[-184]).to eq(code: -184, description: 'Local: Queue full', name: '_QUEUE_FULL') }
    it { expect(errors[21]).to eq(code: 21, description: 'Broker: Invalid required acks value', name: 'INVALID_REQUIRED_ACKS') }
  end

  describe 'admin without auto-start' do
    let(:admin) { config.admin(native_kafka_auto_start: false) }

    it 'expect to be able to start it later and close' do
      admin.start
      admin.close
    end

    it 'expect to be able to close it without starting' do
      admin.close
    end
  end

  describe "#create_topic" do
    describe "called with invalid input" do
      describe "with an invalid topic name" do
        # https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/internals/Topic.java#L29
        # public static final String LEGAL_CHARS = "[a-zA-Z0-9._-]";
        let(:topic_name) { "[!@#]" }

        it "raises an exception" do
          create_topic_handle = admin.create_topic(topic_name, topic_partition_count, topic_replication_factor)
          expect {
            create_topic_handle.wait(max_wait_timeout: 15.0)
          }.to raise_exception { |ex|
            expect(ex).to be_a(Rdkafka::RdkafkaError)
            expect(ex.message).to match(/Broker: Invalid topic \(topic_exception\)/)
            expect(ex.broker_message).to match(/Topic name.*is invalid: .* contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'/)
          }
        end
      end

      describe "with the name of a topic that already exists" do
        let(:topic_name) { "empty_test_topic" } # created in spec_helper.rb

        it "raises an exception" do
          create_topic_handle = admin.create_topic(topic_name, topic_partition_count, topic_replication_factor)
          expect {
            create_topic_handle.wait(max_wait_timeout: 15.0)
          }.to raise_exception { |ex|
            expect(ex).to be_a(Rdkafka::RdkafkaError)
            expect(ex.message).to match(/Broker: Topic already exists \(topic_already_exists\)/)
            expect(ex.broker_message).to match(/Topic 'empty_test_topic' already exists/)
          }
        end
      end

      describe "with an invalid partition count" do
        let(:topic_partition_count) { -999 }

        it "raises an exception" do
          expect {
            admin.create_topic(topic_name, topic_partition_count, topic_replication_factor)
          }.to raise_error Rdkafka::Config::ConfigError, /num_partitions out of expected range/
        end
      end

      describe "with an invalid replication factor" do
        let(:topic_replication_factor) { -2  }

        it "raises an exception" do
          expect {
            admin.create_topic(topic_name, topic_partition_count, topic_replication_factor)
          }.to raise_error Rdkafka::Config::ConfigError, /replication_factor out of expected range/
        end
      end

      describe "with an invalid topic configuration" do
        it "doesn't create the topic" do
          create_topic_handle = admin.create_topic(topic_name, topic_partition_count, topic_replication_factor, invalid_topic_config)
          expect {
            create_topic_handle.wait(max_wait_timeout: 15.0)
          }.to raise_error Rdkafka::RdkafkaError, /Broker: Configuration is invalid \(invalid_config\)/
        end
      end
    end

    context "edge case" do
      context "where we are unable to get the background queue" do
        before do
          allow(Rdkafka::Bindings).to receive(:rd_kafka_queue_get_background).and_return(FFI::Pointer::NULL)
        end

        it "raises an exception" do
          expect {
            admin.create_topic(topic_name, topic_partition_count, topic_replication_factor)
          }.to raise_error Rdkafka::Config::ConfigError, /rd_kafka_queue_get_background was NULL/
        end
      end

      context "where rd_kafka_CreateTopics raises an exception" do
        before do
          allow(Rdkafka::Bindings).to receive(:rd_kafka_CreateTopics).and_raise(RuntimeError.new("oops"))
        end

        it "raises an exception" do
          expect {
            admin.create_topic(topic_name, topic_partition_count, topic_replication_factor)
          }.to raise_error RuntimeError, /oops/
        end
      end
    end

    it "creates a topic" do
      create_topic_handle = admin.create_topic(topic_name, topic_partition_count, topic_replication_factor, topic_config)
      create_topic_report = create_topic_handle.wait(max_wait_timeout: 15.0)
      expect(create_topic_report.error_string).to be_nil
      expect(create_topic_report.result_name).to eq(topic_name)
    end
  end

  describe "describe_configs" do
    subject(:resources_results) { admin.describe_configs(resources).wait.resources }

    before do
      admin.create_topic(topic_name, 2, 1).wait
      sleep(1)
    end

    context 'when describing config of an existing topic' do
      let(:resources) { [{ resource_type: 2, resource_name: topic_name }] }

      it do
        expect(resources_results.size).to eq(1)
        expect(resources_results.first.type).to eq(2)
        expect(resources_results.first.name).to eq(topic_name)
        expect(resources_results.first.configs.size).to be > 25
        expect(resources_results.first.configs.first.name).to eq('compression.type')
        expect(resources_results.first.configs.first.value).to eq('producer')
        expect(resources_results.first.configs.map(&:synonyms)).not_to be_empty
      end
    end

    context 'when describing config of a non-existing topic' do
      let(:resources) { [{ resource_type: 2, resource_name: SecureRandom.uuid }] }

      it 'expect to raise error' do
        expect { resources_results }.to raise_error(Rdkafka::RdkafkaError, /unknown_topic_or_part/)
      end
    end

    context 'when describing both existing and non-existing topics' do
      let(:resources) do
        [
          { resource_type: 2, resource_name: topic_name },
          { resource_type: 2, resource_name: SecureRandom.uuid }
        ]
      end

      it 'expect to raise error' do
        expect { resources_results }.to raise_error(Rdkafka::RdkafkaError, /unknown_topic_or_part/)
      end
    end

    context 'when describing multiple existing topics' do
      let(:resources) do
        [
          { resource_type: 2, resource_name: 'example_topic' },
          { resource_type: 2, resource_name: topic_name }
        ]
      end

      it do
        expect(resources_results.size).to eq(2)
        expect(resources_results.first.type).to eq(2)
        expect(resources_results.first.name).to eq('example_topic')
        expect(resources_results.last.type).to eq(2)
        expect(resources_results.last.name).to eq(topic_name)
      end
    end

    context 'when trying to describe invalid resource type' do
      let(:resources) { [{ resource_type: 0, resource_name: SecureRandom.uuid }] }

      it 'expect to raise error' do
        expect { resources_results }.to raise_error(Rdkafka::RdkafkaError, /invalid_request/)
      end
    end

    context 'when trying to describe invalid broker' do
      let(:resources) { [{ resource_type: 4, resource_name: 'non-existing' }] }

      it 'expect to raise error' do
        expect { resources_results }.to raise_error(Rdkafka::RdkafkaError, /invalid_arg/)
      end
    end

    context 'when trying to describe valid broker' do
      let(:resources) { [{ resource_type: 4, resource_name: '1' }] }

      it do
        expect(resources_results.size).to eq(1)
        expect(resources_results.first.type).to eq(4)
        expect(resources_results.first.name).to eq('1')
        expect(resources_results.first.configs.size).to be > 230
        expect(resources_results.first.configs.first.name).to eq('log.cleaner.min.compaction.lag.ms')
        expect(resources_results.first.configs.first.value).to eq('0')
        expect(resources_results.first.configs.map(&:synonyms)).not_to be_empty
      end
    end

    context 'when describing valid broker with topics in one request' do
      let(:resources) do
        [
          { resource_type: 4, resource_name: '1' },
          { resource_type: 2, resource_name: topic_name }
        ]
      end

      it do
        expect(resources_results.size).to eq(2)
        expect(resources_results.first.type).to eq(4)
        expect(resources_results.first.name).to eq('1')
        expect(resources_results.first.configs.size).to be > 230
        expect(resources_results.first.configs.first.name).to eq('log.cleaner.min.compaction.lag.ms')
        expect(resources_results.first.configs.first.value).to eq('0')
        expect(resources_results.last.type).to eq(2)
        expect(resources_results.last.name).to eq(topic_name)
        expect(resources_results.last.configs.size).to be > 25
        expect(resources_results.last.configs.first.name).to eq('compression.type')
        expect(resources_results.last.configs.first.value).to eq('producer')
      end
    end
  end

  describe "incremental_alter_configs" do
    subject(:resources_results) { admin.incremental_alter_configs(resources_with_configs).wait.resources }

    before do
      admin.create_topic(topic_name, 2, 1).wait
      sleep(1)
    end

    context 'when altering one topic with one valid config via set' do
      let(:target_retention) { (86400002 + rand(10_000)).to_s }
      let(:resources_with_configs) do
        [
          {
            resource_type: 2,
            resource_name: topic_name,
            configs: [
              {
                name: 'delete.retention.ms',
                value: target_retention,
                op_type: 0
              }
            ]
          }
        ]
      end

      it do
        expect(resources_results.size).to eq(1)
        expect(resources_results.first.type).to eq(2)
        expect(resources_results.first.name).to eq(topic_name)

        ret_config = admin.describe_configs(resources_with_configs).wait.resources.first.configs.find do |config|
          config.name == 'delete.retention.ms'
        end

        expect(ret_config.value).to eq(target_retention)
      end
    end

    context 'when altering one topic with one valid config via delete' do
      let(:target_retention) { (8640002 + rand(10_000)).to_s }
      let(:resources_with_configs) do
        [
          {
            resource_type: 2,
            resource_name: topic_name,
            configs: [
              {
                name: 'delete.retention.ms',
                value: target_retention,
                op_type: 1
              }
            ]
          }
        ]
      end

      it do
        expect(resources_results.size).to eq(1)
        expect(resources_results.first.type).to eq(2)
        expect(resources_results.first.name).to eq(topic_name)
        ret_config = admin.describe_configs(resources_with_configs).wait.resources.first.configs.find do |config|
          config.name == 'delete.retention.ms'
        end

        expect(ret_config.value).to eq('86400000')
      end
    end

    context 'when altering one topic with one valid config via append' do
      let(:target_policy) { 'compact' }
      let(:resources_with_configs) do
        [
          {
            resource_type: 2,
            resource_name: topic_name,
            configs: [
              {
                name: 'cleanup.policy',
                value: target_policy,
                op_type: 2
              }
            ]
          }
        ]
      end

      it do
        expect(resources_results.size).to eq(1)
        expect(resources_results.first.type).to eq(2)
        expect(resources_results.first.name).to eq(topic_name)

        ret_config = admin.describe_configs(resources_with_configs).wait.resources.first.configs.find do |config|
          config.name == 'cleanup.policy'
        end

        expect(ret_config.value).to eq("delete,#{target_policy}")
      end
    end

    context 'when altering one topic with one valid config via subtrack' do
      let(:target_policy) { 'delete' }
      let(:resources_with_configs) do
        [
          {
            resource_type: 2,
            resource_name: topic_name,
            configs: [
              {
                name: 'cleanup.policy',
                value: target_policy,
                op_type: 3
              }
            ]
          }
        ]
      end

      it do
        expect(resources_results.size).to eq(1)
        expect(resources_results.first.type).to eq(2)
        expect(resources_results.first.name).to eq(topic_name)

        ret_config = admin.describe_configs(resources_with_configs).wait.resources.first.configs.find do |config|
          config.name == 'cleanup.policy'
        end

        expect(ret_config.value).to eq('')
      end
    end

    context 'when altering one topic with invalid config' do
      let(:target_retention) { '-10' }
      let(:resources_with_configs) do
        [
          {
            resource_type: 2,
            resource_name: topic_name,
            configs: [
              {
                name: 'delete.retention.ms',
                value: target_retention,
                op_type: 0
              }
            ]
          }
        ]
      end

      it 'expect to raise error' do
        expect { resources_results }.to raise_error(Rdkafka::RdkafkaError, /invalid_config/)
      end
    end
  end

  describe "#delete_topic" do
    describe "called with invalid input" do
      # https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/internals/Topic.java#L29
      # public static final String LEGAL_CHARS = "[a-zA-Z0-9._-]";
      describe "with an invalid topic name" do
        let(:topic_name) { "[!@#]" }

        it "raises an exception" do
          delete_topic_handle = admin.delete_topic(topic_name)
          expect {
            delete_topic_handle.wait(max_wait_timeout: 15.0)
          }.to raise_exception { |ex|
            expect(ex).to be_a(Rdkafka::RdkafkaError)
            expect(ex.message).to match(/Broker: Unknown topic or partition \(unknown_topic_or_part\)/)
            expect(ex.broker_message).to match(/Broker: Unknown topic or partition/)
          }
        end
      end

      describe "with the name of a topic that does not exist" do
        it "raises an exception" do
          delete_topic_handle = admin.delete_topic(topic_name)
          expect {
            delete_topic_handle.wait(max_wait_timeout: 15.0)
          }.to raise_exception { |ex|
            expect(ex).to be_a(Rdkafka::RdkafkaError)
            expect(ex.message).to match(/Broker: Unknown topic or partition \(unknown_topic_or_part\)/)
            expect(ex.broker_message).to match(/Broker: Unknown topic or partition/)
          }
        end
      end
    end

    context "edge case" do
      context "where we are unable to get the background queue" do
        before do
          allow(Rdkafka::Bindings).to receive(:rd_kafka_queue_get_background).and_return(FFI::Pointer::NULL)
        end

        it "raises an exception" do
          expect {
            admin.delete_topic(topic_name)
          }.to raise_error Rdkafka::Config::ConfigError, /rd_kafka_queue_get_background was NULL/
        end
      end

      context "where rd_kafka_DeleteTopics raises an exception" do
        before do
          allow(Rdkafka::Bindings).to receive(:rd_kafka_DeleteTopics).and_raise(RuntimeError.new("oops"))
        end

        it "raises an exception" do
          expect {
            admin.delete_topic(topic_name)
          }.to raise_error RuntimeError, /oops/
        end
      end
    end

    it "deletes a topic that was newly created" do
      create_topic_handle = admin.create_topic(topic_name, topic_partition_count, topic_replication_factor)
      create_topic_report = create_topic_handle.wait(max_wait_timeout: 15.0)
      expect(create_topic_report.error_string).to be_nil
      expect(create_topic_report.result_name).to eq(topic_name)

      # Retry topic deletion a few times. On CI Kafka seems to not
      # always be ready for it immediately
      delete_topic_report = nil
      10.times do |i|
        begin
          delete_topic_handle = admin.delete_topic(topic_name)
          delete_topic_report = delete_topic_handle.wait(max_wait_timeout: 15.0)
          break
        rescue Rdkafka::RdkafkaError => ex
          if i > 3
            raise ex
          end
        end
      end

      expect(delete_topic_report.error_string).to be_nil
      expect(delete_topic_report.result_name).to eq(topic_name)
    end
  end

  describe "#ACL tests" do
    let(:non_existing_resource_name) {"non-existing-topic"}
    before do
      #create topic for testing acl
      create_topic_handle = admin.create_topic(resource_name, topic_partition_count, topic_replication_factor)
      create_topic_report = create_topic_handle.wait(max_wait_timeout: 15.0)
    end

    after do
      #delete acl
      delete_acl_handle = admin.delete_acl(resource_type: resource_type, resource_name: resource_name, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
      delete_acl_report = delete_acl_handle.wait(max_wait_timeout: 15.0)

      #delete topic that was created for testing acl
      delete_topic_handle = admin.delete_topic(resource_name)
      delete_topic_report = delete_topic_handle.wait(max_wait_timeout: 15.0)
    end

    describe "#create_acl" do
      it "create acl for a topic that does not exist" do
        # acl creation for resources that does not exist will still get created successfully.
        create_acl_handle = admin.create_acl(resource_type: resource_type, resource_name: non_existing_resource_name, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
        create_acl_report = create_acl_handle.wait(max_wait_timeout: 15.0)
        expect(create_acl_report.rdkafka_response).to eq(0)
        expect(create_acl_report.rdkafka_response_string).to eq("")

        # delete the acl that was created for a non existing topic"
        delete_acl_handle = admin.delete_acl(resource_type: resource_type, resource_name: non_existing_resource_name, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
        delete_acl_report = delete_acl_handle.wait(max_wait_timeout: 15.0)
        expect(delete_acl_handle[:response]).to eq(0)
        expect(delete_acl_report.deleted_acls.size).to eq(1)
      end

      it "creates a acl for topic that was newly created" do
        create_acl_handle = admin.create_acl(resource_type: resource_type, resource_name: resource_name, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
        create_acl_report = create_acl_handle.wait(max_wait_timeout: 15.0)
        expect(create_acl_report.rdkafka_response).to eq(0)
        expect(create_acl_report.rdkafka_response_string).to eq("")
      end
    end

    describe "#describe_acl" do
      it "describe acl of a topic that does not exist" do
        describe_acl_handle = admin.describe_acl(resource_type: resource_type, resource_name: non_existing_resource_name, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
        describe_acl_report = describe_acl_handle.wait(max_wait_timeout: 15.0)
        expect(describe_acl_handle[:response]).to eq(0)
        expect(describe_acl_report.acls.size).to eq(0)
      end

      it "create acls and describe the newly created acls" do
        #create_acl
        create_acl_handle = admin.create_acl(resource_type: resource_type, resource_name: "test_acl_topic_1", resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
        create_acl_report = create_acl_handle.wait(max_wait_timeout: 15.0)
        expect(create_acl_report.rdkafka_response).to eq(0)
        expect(create_acl_report.rdkafka_response_string).to eq("")

        create_acl_handle = admin.create_acl(resource_type: resource_type, resource_name: "test_acl_topic_2", resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
        create_acl_report = create_acl_handle.wait(max_wait_timeout: 15.0)
        expect(create_acl_report.rdkafka_response).to eq(0)
        expect(create_acl_report.rdkafka_response_string).to eq("")

        # Since we create and immediately check, this is slow on loaded CIs, hence we wait
        sleep(2)

        #describe_acl
        describe_acl_handle = admin.describe_acl(resource_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_ANY, resource_name: nil, resource_pattern_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_ANY, principal: nil, host: nil, operation: Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_ANY, permission_type: Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ANY)
        describe_acl_report = describe_acl_handle.wait(max_wait_timeout: 15.0)
        expect(describe_acl_handle[:response]).to eq(0)
        expect(describe_acl_report.acls.length).to eq(2)
      end
    end

    describe "#delete_acl" do
      it "delete acl of a topic that does not exist" do
        delete_acl_handle = admin.delete_acl(resource_type: resource_type, resource_name: non_existing_resource_name, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
        delete_acl_report = delete_acl_handle.wait(max_wait_timeout: 15.0)
        expect(delete_acl_handle[:response]).to eq(0)
        expect(delete_acl_report.deleted_acls.size).to eq(0)
      end

      it "create an acl and delete the newly created acl" do
        #create_acl
        create_acl_handle = admin.create_acl(resource_type: resource_type, resource_name: "test_acl_topic_1", resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
        create_acl_report = create_acl_handle.wait(max_wait_timeout: 15.0)
        expect(create_acl_report.rdkafka_response).to eq(0)
        expect(create_acl_report.rdkafka_response_string).to eq("")

        create_acl_handle = admin.create_acl(resource_type: resource_type, resource_name: "test_acl_topic_2", resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
        create_acl_report = create_acl_handle.wait(max_wait_timeout: 15.0)
        expect(create_acl_report.rdkafka_response).to eq(0)
        expect(create_acl_report.rdkafka_response_string).to eq("")

        #delete_acl - resource_name nil - to delete all acls with any resource name and matching all other filters.
        delete_acl_handle = admin.delete_acl(resource_type: resource_type, resource_name: nil, resource_pattern_type: resource_pattern_type, principal: principal, host: host, operation: operation, permission_type: permission_type)
        delete_acl_report = delete_acl_handle.wait(max_wait_timeout: 15.0)
        expect(delete_acl_handle[:response]).to eq(0)
        expect(delete_acl_report.deleted_acls.length).to eq(2)

      end
    end
  end

  describe('Group tests') do
    describe "#delete_group" do
      describe("with an existing group") do
        let(:consumer_config) { rdkafka_consumer_config('group.id': group_name) }
        let(:producer_config) { rdkafka_producer_config }
        let(:producer) { producer_config.producer }
        let(:consumer) { consumer_config.consumer }

        before do
          # Create a topic, post a message to it, consume it and commit offsets, this will create a group that we can then delete.
          admin.create_topic(topic_name, topic_partition_count, topic_replication_factor).wait(max_wait_timeout: 15.0)

          producer.produce(topic: topic_name, payload: "test", key: "test").wait(max_wait_timeout: 15.0)

          consumer.subscribe(topic_name)
          wait_for_assignment(consumer)
          message = consumer.poll(100)

          expect(message).to_not be_nil

          consumer.commit
          consumer.close
        end

        after do
          producer.close
          consumer.close
        end

        it "deletes the group" do
          delete_group_handle = admin.delete_group(group_name)
          report = delete_group_handle.wait(max_wait_timeout: 15.0)

          expect(report.result_name).to eql(group_name)
        end
      end

      describe "called with invalid input" do
        describe "with the name of a group that does not exist" do
          it "raises an exception" do
            delete_group_handle = admin.delete_group(group_name)

            expect {
              delete_group_handle.wait(max_wait_timeout: 15.0)
            }.to raise_exception { |ex|
              expect(ex).to be_a(Rdkafka::RdkafkaError)
              expect(ex.message).to match(/Broker: The group id does not exist \(group_id_not_found\)/)
            }
          end
        end
      end

    end
  end

  describe '#create_partitions' do
    let(:metadata) { admin.metadata(topic_name).topics.first }

    context 'when topic does not exist' do
      it 'expect to fail due to unknown partition' do
        expect { admin.create_partitions(topic_name, 10).wait }.to raise_error(Rdkafka::RdkafkaError, /unknown_topic_or_part/)
      end
    end

    context 'when topic already has the desired number of partitions' do
      before { admin.create_topic(topic_name, 2, 1).wait }

      it 'expect not to change number of partitions' do
        expect { admin.create_partitions(topic_name, 2).wait }.to raise_error(Rdkafka::RdkafkaError, /invalid_partitions/)
        expect(metadata[:partition_count]).to eq(2)
      end
    end

    context 'when topic has more than the requested number of partitions' do
      before { admin.create_topic(topic_name, 5, 1).wait }

      it 'expect not to change number of partitions' do
        expect { admin.create_partitions(topic_name, 2).wait }.to raise_error(Rdkafka::RdkafkaError, /invalid_partitions/)
        expect(metadata[:partition_count]).to eq(5)
      end
    end

    context 'when topic has less then desired number of partitions' do
      before do
        admin.create_topic(topic_name, 1, 1).wait
        sleep(1)
      end

      it 'expect to change number of partitions' do
        admin.create_partitions(topic_name, 10).wait
        expect(metadata[:partition_count]).to eq(10)
      end
    end
  end

  describe '#oauthbearer_set_token' do
    context 'when sasl not configured' do
      it 'should return RD_KAFKA_RESP_ERR__STATE' do
        response = admin.oauthbearer_set_token(
          token: "foo",
          lifetime_ms: Time.now.to_i*1000 + 900 * 1000,
          principal_name: "kafka-cluster"
        )
        expect(response).to eq(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE)
      end
    end

    context 'when sasl configured' do
      before do
        config_sasl = rdkafka_config(
          "security.protocol": "sasl_ssl",
          "sasl.mechanisms": 'OAUTHBEARER'
        )
        $admin_sasl = config_sasl.admin
      end

      after do
        $admin_sasl.close
      end

      it 'should succeed' do

        response = $admin_sasl.oauthbearer_set_token(
          token: "foo",
          lifetime_ms: Time.now.to_i*1000 + 900 * 1000,
          principal_name: "kafka-cluster"
        )
        expect(response).to eq(0)
      end
    end
  end

  unless RUBY_PLATFORM == 'java'
    context "when operating from a fork" do
      # @see https://github.com/ffi/ffi/issues/1114
      it 'expect to be able to create topics and run other admin operations without hanging' do
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
