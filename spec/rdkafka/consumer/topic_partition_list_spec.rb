require "spec_helper"

describe Rdkafka::Consumer::TopicPartitionList do
  it "should create a list from an existing native list" do
    pointer = Rdkafka::Bindings.rd_kafka_topic_partition_list_new(5)
    Rdkafka::Bindings.rd_kafka_topic_partition_list_add(
      pointer,
      "topic",
      -1
    )
    list = Rdkafka::Consumer::TopicPartitionList.new(pointer)

    other = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
      list.add_topic("topic")
    end

    expect(list).to eq other
  end

  it "should create a new list and add unassigned topics" do
    list = Rdkafka::Consumer::TopicPartitionList.new

    expect(list.count).to eq 0
    expect(list.empty?).to be true

    list.add_topic("topic1")
    list.add_topic("topic2")

    expect(list.count).to eq 2
    expect(list.empty?).to be false

    hash = list.to_h
    expect(hash.count).to eq 2
    expect(hash).to eq ({
      "topic1" => nil,
      "topic2" => nil
    })
  end

  it "should create a new list and add assigned topics" do
    list = Rdkafka::Consumer::TopicPartitionList.new

    expect(list.count).to eq 0
    expect(list.empty?).to be true

    list.add_topic("topic1", [0, 1, 2])
    list.add_topic("topic2", [0, 1])

    expect(list.count).to eq 5
    expect(list.empty?).to be false

    hash = list.to_h
    expect(hash.count).to eq 2
    expect(hash["topic1"]).to eq([
      Rdkafka::Consumer::Partition.new(0, -1001),
      Rdkafka::Consumer::Partition.new(1, -1001),
      Rdkafka::Consumer::Partition.new(2, -1001)
    ])
    expect(hash["topic2"]).to eq([
      Rdkafka::Consumer::Partition.new(0, -1001),
      Rdkafka::Consumer::Partition.new(1, -1001)
    ])
  end

  describe "#to_s" do
    it "should return a human readable representation" do
      list = Rdkafka::Consumer::TopicPartitionList.new
      list.add_topic("topic1", [0, 1])

      expected = "<TopicPartitionList: {\"topic1\"=>[<Partition 0 with offset -1001>, <Partition 1 with offset -1001>]}>"

      expect(list.to_s).to eq expected
    end
  end

  describe "#==" do
    subject do
      Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic("topic1", [0])
      end
    end

    it "should equal another partition with the same content" do
      other = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic("topic1", [0])
      end
      expect(subject).to eq other
    end

    it "should not equal another partition with different content" do
      expect(subject).not_to eq Rdkafka::Consumer::TopicPartitionList.new
    end
  end
end
