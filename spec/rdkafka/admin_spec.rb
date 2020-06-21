require "spec_helper"
require "ostruct"

describe Rdkafka::Admin do
  let(:config)   { rdkafka_config }

  let(:admin)    { config.admin }
  after          { admin.close }

  let(:topic_name)               { "test-topic-#{Random.new.rand(0..1_000_000)}" }
  let(:topic_partition_count)    { 3 }
  let(:topic_replication_factor) { 1 }

  describe "#create_topic" do

    describe "called with invalid input" do

      # https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/internals/Topic.java#L29
      # public static final String LEGAL_CHARS = "[a-zA-Z0-9._-]";
      describe "with an invalid topic name" do
        let(:topic_name) { "[!@#]" }

        it "raises an exception" do
          create_topic_handle = admin.create_topic(topic_name, topic_partition_count, topic_replication_factor)
          expect {
            create_topic_handle.wait(max_wait_timeout: 5.0)
          }.to raise_exception { |ex|
            expect(ex).to be_a(Rdkafka::RdkafkaError)
            expect(ex.message).to match(/Broker: Invalid topic \(topic_exception\)/)
            expect(ex.broker_message).to match(/Topic name.*is illegal, it contains a character other than ASCII alphanumerics/)
          }
        end
      end

      describe "with an invalid partition count" do
        let(:topic_partition_count) { -1 }

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
    end

    it "creates a topic" do
      create_topic_handle = admin.create_topic(topic_name, topic_partition_count, topic_replication_factor)
      create_topic_report = create_topic_handle.wait(max_wait_timeout: 5.0)
      expect(create_topic_report.error_string).to be_nil
      expect(create_topic_report.result_name).to eq(topic_name)
    end
  end

  # TODO GT write specs for delete_topic
  # describe "#delete_topic" do
  #   it "works!" do
  #   end
  # end
end
