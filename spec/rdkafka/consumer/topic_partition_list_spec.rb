require "spec_helper"

describe Rdkafka::Consumer::TopicPartitionList do
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

  it "should create a new list and add assigned topics as a range" do
    list = Rdkafka::Consumer::TopicPartitionList.new

    expect(list.count).to eq 0
    expect(list.empty?).to be true

    list.add_topic("topic1", (0..2))
    list.add_topic("topic2", (0..1))

    expect(list.count).to eq 5
    expect(list.empty?).to be false

    hash = list.to_h
    expect(hash.count).to eq 2
    expect(hash["topic1"]).to eq([
      Rdkafka::Consumer::Partition.new(0, nil),
      Rdkafka::Consumer::Partition.new(1, nil),
      Rdkafka::Consumer::Partition.new(2, nil)
    ])
    expect(hash["topic2"]).to eq([
      Rdkafka::Consumer::Partition.new(0, nil),
      Rdkafka::Consumer::Partition.new(1, nil)
    ])
  end

  it "should create a new list and add assigned topics as an array" do
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
      Rdkafka::Consumer::Partition.new(0, nil),
      Rdkafka::Consumer::Partition.new(1, nil),
      Rdkafka::Consumer::Partition.new(2, nil)
    ])
    expect(hash["topic2"]).to eq([
      Rdkafka::Consumer::Partition.new(0, nil),
      Rdkafka::Consumer::Partition.new(1, nil)
    ])
  end

  it "should create a new list and add assigned topics as a count" do
    list = Rdkafka::Consumer::TopicPartitionList.new

    expect(list.count).to eq 0
    expect(list.empty?).to be true

    list.add_topic("topic1", 3)
    list.add_topic("topic2", 2)

    expect(list.count).to eq 5
    expect(list.empty?).to be false

    hash = list.to_h
    expect(hash.count).to eq 2
    expect(hash["topic1"]).to eq([
      Rdkafka::Consumer::Partition.new(0, nil),
      Rdkafka::Consumer::Partition.new(1, nil),
      Rdkafka::Consumer::Partition.new(2, nil)
    ])
    expect(hash["topic2"]).to eq([
      Rdkafka::Consumer::Partition.new(0, nil),
      Rdkafka::Consumer::Partition.new(1, nil)
    ])
  end

  it "should create a new list and add topics and partitions with an offset" do
    list = Rdkafka::Consumer::TopicPartitionList.new

    expect(list.count).to eq 0
    expect(list.empty?).to be true

    list.add_topic_and_partitions_with_offsets("topic1", 0 => 5, 1 => 6, 2 => 7)

    hash = list.to_h
    expect(hash.count).to eq 1
    expect(hash["topic1"]).to eq([
      Rdkafka::Consumer::Partition.new(0, 5),
      Rdkafka::Consumer::Partition.new(1, 6),
      Rdkafka::Consumer::Partition.new(2, 7)
    ])
  end

  describe "#to_s" do
    it "should return a human readable representation" do
      list = Rdkafka::Consumer::TopicPartitionList.new
      list.add_topic("topic1", [0, 1])

      expected = "<TopicPartitionList: {\"topic1\"=>[<Partition 0>, <Partition 1>]}>"

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

  describe ".from_native_tpl" do
    it "should create a list from an existing native list" do
      pointer = Rdkafka::Bindings.rd_kafka_topic_partition_list_new(5)
      Rdkafka::Bindings.rd_kafka_topic_partition_list_add(
        pointer,
        "topic",
        -1
      )
      list = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(pointer)

      other = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic("topic")
      end

      expect(list).to eq other
    end

    it "should create a list from an existing native list with offsets" do
      pointer = Rdkafka::Bindings.rd_kafka_topic_partition_list_new(5)
      Rdkafka::Bindings.rd_kafka_topic_partition_list_add(
        pointer,
        "topic",
        0
      )
      Rdkafka::Bindings.rd_kafka_topic_partition_list_set_offset(
        pointer,
        "topic",
        0,
        100
      )
      list = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(pointer)

      other = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic_and_partitions_with_offsets("topic", 0 => 100)
      end

      expect(list).to eq other
    end
  end

  describe "#to_native_tpl" do
    it "should create a native list" do
      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic("topic")
      end

      tpl = list.to_native_tpl

      other = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(tpl)

      expect(list).to eq other
    end

    it "should create a native list with partitions" do
      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic("topic", 0..16)
      end

      tpl = list.to_native_tpl

      other = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(tpl)

      expect(list).to eq other
    end

    it "should create a native list with offsets" do
      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic_and_partitions_with_offsets("topic", 0 => 100)
      end

      tpl = list.to_native_tpl

      other = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(tpl)

      expect(list).to eq other
    end
  end
end
