require "spec_helper"

describe "AdminOperations" do
  let(:config) { rdkafka_config }
  let(:admin) { producer }
  let(:consumer) { config.consumer }
  let(:producer) { config.producer }

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

  describe "#query_watermark_offsets" do
    it "should return the watermark offsets" do
      # Make sure there's a message
      producer.produce(
        topic:     "watermarks_test_topic",
        payload:   "payload 1",
        key:       "key 1",
        partition: 0
      ).wait

      low, high = consumer.query_watermark_offsets("watermarks_test_topic", 0, 5000)
      expect(low).to eq 0
      expect(high).to be > 0

      low, high = producer.query_watermark_offsets("watermarks_test_topic", 0, 5000)
      expect(low).to eq 0
      expect(high).to be > 0
    end

    it "should raise an error when consumer querying offsets fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_query_watermark_offsets).and_return(20)
      expect {
        consumer.query_watermark_offsets("consume_test_topic", 0, 5000)
      }.to raise_error Rdkafka::RdkafkaError
    end

    it "should raise an error when producer querying offsets fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_query_watermark_offsets).and_return(20)
      expect {
        producer.query_watermark_offsets("consume_test_topic", 0, 5000)
      }.to raise_error Rdkafka::RdkafkaError
    end
  end

  describe "#lag" do
    let(:config) { rdkafka_config(:"enable.partition.eof" => true) }

    it "should calculate the consumer lag" do
      # Make sure there's a message in every partition and
      # wait for the message to make sure everything is committed.
      (0..2).each do |i|
        report = producer.produce(
          topic:     "consume_test_topic",
          key:       "key lag #{i}",
          partition: i
        ).wait
      end

      # Consume to the end
      consumer.subscribe("consume_test_topic")
      eof_count = 0
      loop do
        begin
          consumer.poll(100)
        rescue Rdkafka::RdkafkaError => error
          if error.is_partition_eof?
            eof_count += 1
          end
          break if eof_count == 3
        end
      end

      # Commit
      consumer.commit

      # Create list to fetch lag for. TODO creating the list will not be necessary
      # after committed uses the subscription.
      list = consumer.committed(Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic("consume_test_topic", (0..2))
      end)

      # Lag should be 0 now
      expected_lag = {
        "consume_test_topic" => {
          0 => 0,
          1 => 0,
          2 => 0
        }
      }
      expect(consumer.lag(list)).to eq(expected_lag)
      expect(producer.lag(list)).to eq(expected_lag)

      # Produce message on every topic again
      (0..2).each do |i|
        report = producer.produce(
          topic:     "consume_test_topic",
          key:       "key lag #{i}",
          partition: i
        ).wait
      end

      # Lag should be 1 now
      expected_lag = {
        "consume_test_topic" => {
          0 => 1,
          1 => 1,
          2 => 1
        }
      }
      expect(consumer.lag(list)).to eq(expected_lag)
      expect(producer.lag(list)).to eq(expected_lag)
    end

    it "returns nil if there are no messages on the topic" do
      list = consumer.committed(Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic("consume_test_topic", (0..2))
      end)

      expected_lag = {
        "consume_test_topic" => {}
      }
      expect(consumer.lag(list)).to eq(expected_lag)
      expect(producer.lag(list)).to eq(expected_lag)
    end
  end
end
