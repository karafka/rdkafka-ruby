# frozen_string_literal: true

require "ostruct"

describe Rdkafka::Admin do
  let(:config) { rdkafka_config }
  let(:admin)  { config.admin }

  after do
    # Registry should always end up being empty
    expect(Rdkafka::Admin::CreateTopicHandle::REGISTRY).to be_empty
    expect(Rdkafka::Admin::DescribeAclHandle::REGISTRY).to be_empty
    expect(Rdkafka::Admin::CreateAclHandle::REGISTRY).to be_empty
    expect(Rdkafka::Admin::DeleteAclHandle::REGISTRY).to be_empty
    admin.close
  end

  let(:topic_name)               { "test-topic-#{Random.new.rand(0..1_000_000)}" }
  let(:topic_partition_count)    { 3 }
  let(:topic_replication_factor) { 1 }
  let(:topic_config)             { {"cleanup.policy" => "compact", "min.cleanable.dirty.ratio" => 0.8} }
  let(:invalid_topic_config)     { {"cleeeeenup.policee" => "campact"} }
  let(:group_name)               { "test-group-#{Random.new.rand(0..1_000_000)}" }

  let(:resource_name)         {"acl-test-topic"}
  let(:resource_type)         {Rdkafka::Bindings::RD_KAFKA_RESOURCE_TOPIC}
  let(:resource_pattern_type) {Rdkafka::Bindings::RD_KAFKA_RESOURCE_PATTERN_LITERAL}
  let(:principal)             {"User:anonymous"}
  let(:host)                  {"*"}
  let(:operation)             {Rdkafka::Bindings::RD_KAFKA_ACL_OPERATION_READ}
  let(:permission_type)       {Rdkafka::Bindings::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW}

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
end
