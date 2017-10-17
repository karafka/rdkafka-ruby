require "spec_helper"

describe Rdkafka::Consumer do
  let(:config) { rdkafka_config }
  let(:consumer) { config.consumer }
  let(:producer) { config.producer }

  context "subscription" do
    it "should subscribe" do
      expect(consumer.subscription).to be_empty

      consumer.subscribe("consume_test_topic")

      expect(consumer.subscription).not_to be_empty
      expected_subscription = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic("consume_test_topic")
      end
      expect(consumer.subscription).to eq expected_subscription

      consumer.unsubscribe

      expect(consumer.subscription).to be_empty
    end
  end

  describe "committed" do
    before do
      # Make sure there's a stored offset
      report = producer.produce(
        topic:     "consume_test_topic",
        payload:   "payload 1",
        key:       "key 1",
        partition: 0
      ).wait
      message = wait_for_message(
        topic: "consume_test_topic",
        delivery_report: report,
        config: config
      )
    end

    it "should fetch the committed offsets for a specified topic partition list" do
      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic("consume_test_topic", [0, 1, 2])
      end
      partitions = consumer.committed(list).to_h["consume_test_topic"]
      expect(partitions[0].offset).to be > 0
      expect(partitions[1].offset).to eq -1001
      expect(partitions[2].offset).to eq -1001
    end
  end

  describe "watermark offsets" do
    it "should return the watermark offsets" do
      # Make sure there's a message
      producer.produce(
        topic:     "consume_test_topic",
        payload:   "payload 1",
        key:       "key 1",
        partition: 0
      ).wait

      low, high = consumer.query_watermark_offsets("consume_test_topic", 0, 5000)
      expect(low).to eq 0
      expect(high).to be > 0
    end
  end
end
