require "spec_helper"

describe "AdminOperations" do
  let(:admin) { rdkafka_config.producer }

  describe "#create_topic and #delete_topic" do
    context "when topic does not exist" do
      before do
        # Remove admin_new_topic if it already exists first
        begin
          admin.delete_topic("admin_new_topic")
          # Wait a little to allow Kafka to catch up
          sleep 1
        rescue Rdkafka::RdkafkaError
          # Ignore error while deleting
        end
      end

      it "should succeed" do
        admin.create_topic("admin_new_topic", config: {"retention.ms": "12345"})

        # Wait a little to allow Kafka to catch up
        sleep 1

        expect(admin.describe_topic("admin_new_topic")["retention.ms"]).to eq("12345")
        admin.alter_topic("admin_new_topic", {"retention.ms": "56789"})
        expect(admin.describe_topic("admin_new_topic")["retention.ms"]).to eq("56789")

        expect do
          admin.create_partitions_for("admin_new_topic", num_partitions: 1)
        end.to raise_error(Rdkafka::RdkafkaError, /invalid_partitions/)

        metadata = admin.metadata_for("admin_new_topic")
        expect(metadata.partitions.count).to eq(1)

        admin.create_partitions_for("admin_new_topic", num_partitions: 8)

        metadata = admin.metadata_for("admin_new_topic")
        expect(metadata.partitions.count).to eq(8)

        admin.delete_topic("admin_new_topic")
      end
    end

    context "when topic exists" do
      it "should raise an error" do
        expect do
          admin.create_topic("empty_test_topic")
        end.to raise_error(Rdkafka::RdkafkaError, "Topic 'empty_test_topic' already exists. - Broker: Topic already exists (topic_already_exists)")
      end
    end
  end

  describe "#describe_topic" do
    context "when topic does not exist" do
      it "should raise an error" do
        expect do
          admin.describe_topic("i_dont_exist")
        end.to raise_error(Rdkafka::RdkafkaError, "Broker: Unknown topic or partition - Broker: Unknown topic or partition (unknown_topic_or_part)")
      end
    end

    context "when topic exists" do
      it "should succeed" do
        config = admin.describe_topic("empty_test_topic")
        expect(config.keys).not_to be_empty
        expect(config["min.insync.replicas"]).to eq("1")
      end
    end

    context "when kafka brokers do not exist" do
      it "should time out" do
        admin = Rdkafka::Config.new("bootstrap.servers": "i_dont_exist:9099").producer
        expect do
          admin.describe_topic("i_dont_exist", timeout: 2)
        end.to raise_error(Rdkafka::RdkafkaError, /timed_out/)
      end
    end
  end
end
