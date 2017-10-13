require "spec_helper"

describe Rdkafka::Consumer do
  let(:consumer) { rdkafka_config.consumer }

  context "subscription" do
    it "should subscribe" do
      expect(consumer.subscription).to be_empty

      consumer.subscribe("consume_test_topic")

      expect(consumer.subscription).not_to be_empty
      expected_subscription = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_unassigned_topic("consume_test_topic")
      end
      expect(consumer.subscription).to eq expected_subscription

      consumer.unsubscribe

      expect(consumer.subscription).to be_empty
    end
  end
end
