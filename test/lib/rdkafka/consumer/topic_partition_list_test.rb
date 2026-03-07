# frozen_string_literal: true

require "test_helper"

class TopicPartitionListTest < Minitest::Test
  def test_creates_list_with_unassigned_topics
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

  def test_creates_list_with_assigned_topics_as_range
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

  def test_creates_list_with_assigned_topics_as_array
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

  def test_creates_list_with_assigned_topics_as_count
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

  def test_creates_list_with_partitions_and_offsets
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

  def test_to_s
    expected = if RUBY_VERSION >= "3.4.0"
      '<TopicPartitionList: {"topic1" => [<Partition 0>, <Partition 1>]}>'
    else
      '<TopicPartitionList: {"topic1"=>[<Partition 0>, <Partition 1>]}>'
    end

    list = Rdkafka::Consumer::TopicPartitionList.new
    list.add_topic("topic1", [0, 1])

    assert_equal expected, list.to_s
  end

  def test_equals_same_content
    subject = Rdkafka::Consumer::TopicPartitionList.new.tap { |l| l.add_topic("topic1", [0]) }
    other = Rdkafka::Consumer::TopicPartitionList.new.tap { |l| l.add_topic("topic1", [0]) }

    assert_equal other, subject
  end

  def test_not_equals_different_content
    subject = Rdkafka::Consumer::TopicPartitionList.new.tap { |l| l.add_topic("topic1", [0]) }

    refute_equal Rdkafka::Consumer::TopicPartitionList.new, subject
  end

  def test_from_native_tpl
    pointer = Rdkafka::Bindings.rd_kafka_topic_partition_list_new(5)
    Rdkafka::Bindings.rd_kafka_topic_partition_list_add(pointer, "topic", -1)
    list = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(pointer)

    other = Rdkafka::Consumer::TopicPartitionList.new.tap { |l| l.add_topic("topic") }

    assert_equal other, list
  end

  def test_from_native_tpl_with_offsets
    pointer = Rdkafka::Bindings.rd_kafka_topic_partition_list_new(5)
    Rdkafka::Bindings.rd_kafka_topic_partition_list_add(pointer, "topic", 0)
    Rdkafka::Bindings.rd_kafka_topic_partition_list_set_offset(pointer, "topic", 0, 100)
    list = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(pointer)

    other = Rdkafka::Consumer::TopicPartitionList.new.tap { |l| l.add_topic_and_partitions_with_offsets("topic", 0 => 100) }

    assert_equal other, list
  end

  def test_to_native_tpl
    list = Rdkafka::Consumer::TopicPartitionList.new.tap { |l| l.add_topic("topic") }
    tpl = list.to_native_tpl
    other = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(tpl)

    assert_equal list, other
  end

  def test_to_native_tpl_with_partitions
    list = Rdkafka::Consumer::TopicPartitionList.new.tap { |l| l.add_topic("topic", 0..16) }
    tpl = list.to_native_tpl
    other = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(tpl)

    assert_equal list, other
  end

  def test_to_native_tpl_with_offsets
    list = Rdkafka::Consumer::TopicPartitionList.new.tap { |l| l.add_topic_and_partitions_with_offsets("topic", 0 => 100) }
    tpl = list.to_native_tpl
    other = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(tpl)

    assert_equal list, other
  end

  def test_to_native_tpl_with_timestamp_offsets
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
