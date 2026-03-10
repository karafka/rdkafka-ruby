# frozen_string_literal: true

describe Rdkafka::Consumer::TopicPartitionList do
  it "creates a new list and add unassigned topics" do
    list = Rdkafka::Consumer::TopicPartitionList.new

    assert_equal 0, list.count
    assert_empty list

    list.add_topic("topic1")
    list.add_topic("topic2")

    assert_equal 2, list.count
    refute_empty list

    hash = list.to_h

    assert_equal 2, hash.count
    assert_equal({ "topic1" => nil, "topic2" => nil }, hash)
  end

  it "creates a new list and add assigned topics as a range" do
    list = Rdkafka::Consumer::TopicPartitionList.new

    assert_equal 0, list.count
    assert_empty list

    list.add_topic("topic1", 0..2)
    list.add_topic("topic2", 0..1)

    assert_equal 5, list.count
    refute_empty list

    hash = list.to_h

    assert_equal 2, hash.count
    assert_equal([
      Rdkafka::Consumer::Partition.new(0, nil),
      Rdkafka::Consumer::Partition.new(1, nil),
      Rdkafka::Consumer::Partition.new(2, nil)
    ], hash["topic1"])
    assert_equal([
      Rdkafka::Consumer::Partition.new(0, nil),
      Rdkafka::Consumer::Partition.new(1, nil)
    ], hash["topic2"])
  end

  it "creates a new list and add assigned topics as an array" do
    list = Rdkafka::Consumer::TopicPartitionList.new

    assert_equal 0, list.count
    assert_empty list

    list.add_topic("topic1", [0, 1, 2])
    list.add_topic("topic2", [0, 1])

    assert_equal 5, list.count
    refute_empty list

    hash = list.to_h

    assert_equal 2, hash.count
    assert_equal([
      Rdkafka::Consumer::Partition.new(0, nil),
      Rdkafka::Consumer::Partition.new(1, nil),
      Rdkafka::Consumer::Partition.new(2, nil)
    ], hash["topic1"])
    assert_equal([
      Rdkafka::Consumer::Partition.new(0, nil),
      Rdkafka::Consumer::Partition.new(1, nil)
    ], hash["topic2"])
  end

  it "creates a new list and add assigned topics as a count" do
    list = Rdkafka::Consumer::TopicPartitionList.new

    assert_equal 0, list.count
    assert_empty list

    list.add_topic("topic1", 3)
    list.add_topic("topic2", 2)

    assert_equal 5, list.count
    refute_empty list

    hash = list.to_h

    assert_equal 2, hash.count
    assert_equal([
      Rdkafka::Consumer::Partition.new(0, nil),
      Rdkafka::Consumer::Partition.new(1, nil),
      Rdkafka::Consumer::Partition.new(2, nil)
    ], hash["topic1"])
    assert_equal([
      Rdkafka::Consumer::Partition.new(0, nil),
      Rdkafka::Consumer::Partition.new(1, nil)
    ], hash["topic2"])
  end

  it "creates a new list and add topics and partitions with an offset" do
    list = Rdkafka::Consumer::TopicPartitionList.new

    assert_equal 0, list.count
    assert_empty list

    list.add_topic_and_partitions_with_offsets("topic1", 0 => 5, 1 => 6, 2 => 7)

    hash = list.to_h

    assert_equal 1, hash.count
    assert_equal([
      Rdkafka::Consumer::Partition.new(0, 5),
      Rdkafka::Consumer::Partition.new(1, 6),
      Rdkafka::Consumer::Partition.new(2, 7)
    ], hash["topic1"])
  end

  describe "#to_s" do
    it "returns a human readable representation" do
      expected = if RUBY_VERSION >= "3.4.0"
        '<TopicPartitionList: {"topic1" => [<Partition 0>, <Partition 1>]}>'
      else
        '<TopicPartitionList: {"topic1"=>[<Partition 0>, <Partition 1>]}>'
      end

      list = Rdkafka::Consumer::TopicPartitionList.new
      list.add_topic("topic1", [0, 1])

      assert_equal expected, list.to_s
    end
  end

  describe "#==" do
    it "equals another partition with the same content" do
      subject = Rdkafka::Consumer::TopicPartitionList.new.tap { |l| l.add_topic("topic1", [0]) }
      other = Rdkafka::Consumer::TopicPartitionList.new.tap { |l| l.add_topic("topic1", [0]) }

      assert_equal other, subject
    end

    it "does not equal another partition with different content" do
      subject = Rdkafka::Consumer::TopicPartitionList.new.tap { |l| l.add_topic("topic1", [0]) }

      refute_equal Rdkafka::Consumer::TopicPartitionList.new, subject
    end
  end

  describe ".from_native_tpl" do
    it "creates a list from an existing native list" do
      pointer = Rdkafka::Bindings.rd_kafka_topic_partition_list_new(5)
      Rdkafka::Bindings.rd_kafka_topic_partition_list_add(pointer, "topic", -1)
      list = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(pointer)

      other = Rdkafka::Consumer::TopicPartitionList.new.tap { |l| l.add_topic("topic") }

      assert_equal other, list
    end

    it "creates a list from an existing native list with offsets" do
      pointer = Rdkafka::Bindings.rd_kafka_topic_partition_list_new(5)
      Rdkafka::Bindings.rd_kafka_topic_partition_list_add(pointer, "topic", 0)
      Rdkafka::Bindings.rd_kafka_topic_partition_list_set_offset(pointer, "topic", 0, 100)
      list = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(pointer)

      other = Rdkafka::Consumer::TopicPartitionList.new.tap { |l| l.add_topic_and_partitions_with_offsets("topic", 0 => 100) }

      assert_equal other, list
    end
  end

  describe "#to_native_tpl" do
    it "creates a native list" do
      list = Rdkafka::Consumer::TopicPartitionList.new.tap { |l| l.add_topic("topic") }
      tpl = list.to_native_tpl
      other = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(tpl)

      assert_equal list, other
    end

    it "creates a native list with partitions" do
      list = Rdkafka::Consumer::TopicPartitionList.new.tap { |l| l.add_topic("topic", 0..16) }
      tpl = list.to_native_tpl
      other = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(tpl)

      assert_equal list, other
    end

    it "creates a native list with offsets" do
      list = Rdkafka::Consumer::TopicPartitionList.new.tap { |l| l.add_topic_and_partitions_with_offsets("topic", 0 => 100) }
      tpl = list.to_native_tpl
      other = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(tpl)

      assert_equal list, other
    end

    it "creates a native list with timestamp offsets if offsets are Time" do
      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic_and_partitions_with_offsets("topic", 0 => Time.at(1505069646, 250_000))
      end

      tpl = list.to_native_tpl

      compare_list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic_and_partitions_with_offsets(
          "topic",
          0 => (Time.at(1505069646, 250_000).to_f * 1000).floor
        )
      end

      native_list = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(tpl)

      assert_equal compare_list, native_list
    end
  end
end
