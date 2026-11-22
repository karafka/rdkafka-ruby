# frozen_string_literal: true

describe Rdkafka::Consumer::TopicPartitionList do
  it "creates a new list and add unassigned topics" do
    list = described_class.new

    assert_equal(0, list.count)
    assert_empty(list)

    list.add_topic("topic1")
    list.add_topic("topic2")

    assert_equal(2, list.count)
    refute_empty(list)

    hash = list.to_h

    assert_equal(2, hash.count)
    assert_equal(
      {
        "topic1" => nil,
        "topic2" => nil
      },
      hash
    )
  end

  it "creates a new list and add assigned topics as a range" do
    list = described_class.new

    assert_equal(0, list.count)
    assert_empty(list)

    list.add_topic("topic1", 0..2)
    list.add_topic("topic2", 0..1)

    assert_equal(5, list.count)
    refute_empty(list)

    hash = list.to_h

    assert_equal(2, hash.count)
    assert_equal(
      [
        Rdkafka::Consumer::Partition.new(0, nil),
        Rdkafka::Consumer::Partition.new(1, nil),
        Rdkafka::Consumer::Partition.new(2, nil)
      ],
      hash["topic1"]
    )
    assert_equal(
      [
        Rdkafka::Consumer::Partition.new(0, nil),
        Rdkafka::Consumer::Partition.new(1, nil)
      ],
      hash["topic2"]
    )
  end

  it "creates a new list and add assigned topics as an array" do
    list = described_class.new

    assert_equal(0, list.count)
    assert_empty(list)

    list.add_topic("topic1", [0, 1, 2])
    list.add_topic("topic2", [0, 1])

    assert_equal(5, list.count)
    refute_empty(list)

    hash = list.to_h

    assert_equal(2, hash.count)
    assert_equal(
      [
        Rdkafka::Consumer::Partition.new(0, nil),
        Rdkafka::Consumer::Partition.new(1, nil),
        Rdkafka::Consumer::Partition.new(2, nil)
      ],
      hash["topic1"]
    )
    assert_equal(
      [
        Rdkafka::Consumer::Partition.new(0, nil),
        Rdkafka::Consumer::Partition.new(1, nil)
      ],
      hash["topic2"]
    )
  end

  it "creates a new list and add assigned topics as a count" do
    list = described_class.new

    assert_equal(0, list.count)
    assert_empty(list)

    list.add_topic("topic1", 3)
    list.add_topic("topic2", 2)

    assert_equal(5, list.count)
    refute_empty(list)

    hash = list.to_h

    assert_equal(2, hash.count)
    assert_equal(
      [
        Rdkafka::Consumer::Partition.new(0, nil),
        Rdkafka::Consumer::Partition.new(1, nil),
        Rdkafka::Consumer::Partition.new(2, nil)
      ],
      hash["topic1"]
    )
    assert_equal(
      [
        Rdkafka::Consumer::Partition.new(0, nil),
        Rdkafka::Consumer::Partition.new(1, nil)
      ],
      hash["topic2"]
    )
  end

  it "creates a new list and add topics and partitions with an offset" do
    list = described_class.new

    assert_equal(0, list.count)
    assert_empty(list)

    list.add_topic_and_partitions_with_offsets("topic1", 0 => 5, 1 => 6, 2 => 7)

    hash = list.to_h

    assert_equal(1, hash.count)
    assert_equal(
      [
        Rdkafka::Consumer::Partition.new(0, 5),
        Rdkafka::Consumer::Partition.new(1, 6),
        Rdkafka::Consumer::Partition.new(2, 7)
      ],
      hash["topic1"]
    )
  end

  describe "#to_s" do
    before do
      @expected = if RUBY_VERSION >= "3.4.0"
        "<TopicPartitionList: {\"topic1\" => [<Partition 0>, <Partition 1>]}>"
      else
        "<TopicPartitionList: {\"topic1\"=>[<Partition 0>, <Partition 1>]}>"
      end
    end

    it "returns a human readable representation" do
      list = described_class.new
      list.add_topic("topic1", [0, 1])

      assert_equal(@expected, list.to_s)
    end
  end

  describe "#==" do
    before do
      @list = described_class.new.tap do |list|
        list.add_topic("topic1", [0])
      end
    end

    it "equals another partition with the same content" do
      other = described_class.new.tap do |list|
        list.add_topic("topic1", [0])
      end

      assert_equal(other, @list)
    end

    it "does not equal another partition with different content" do
      refute_equal(described_class.new, @list)
    end
  end

  describe ".from_native_tpl" do
    it "creates a list from an existing native list" do
      pointer = Rdkafka::Bindings.rd_kafka_topic_partition_list_new(5)
      Rdkafka::Bindings.rd_kafka_topic_partition_list_add(
        pointer,
        "topic",
        -1
      )
      list = described_class.from_native_tpl(pointer)

      other = described_class.new.tap do |list|
        list.add_topic("topic")
      end

      assert_equal(other, list)
    end

    it "creates a list from an existing native list with offsets" do
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
      list = described_class.from_native_tpl(pointer)

      other = described_class.new.tap do |list|
        list.add_topic_and_partitions_with_offsets("topic", 0 => 100)
      end

      assert_equal(other, list)
    end
  end

  describe "#to_native_tpl" do
    it "creates a native list" do
      list = described_class.new.tap do |list|
        list.add_topic("topic")
      end

      tpl = list.to_native_tpl

      other = described_class.from_native_tpl(tpl)

      assert_equal(other, list)
    end

    it "creates a native list with partitions" do
      list = described_class.new.tap do |list|
        list.add_topic("topic", 0..16)
      end

      tpl = list.to_native_tpl

      other = described_class.from_native_tpl(tpl)

      assert_equal(other, list)
    end

    it "creates a native list with offsets" do
      list = described_class.new.tap do |list|
        list.add_topic_and_partitions_with_offsets("topic", 0 => 100)
      end

      tpl = list.to_native_tpl

      other = described_class.from_native_tpl(tpl)

      assert_equal(other, list)
    end

    it "creates a native list with timetamp offsets if offsets are Time" do
      list = described_class.new.tap do |list|
        list.add_topic_and_partitions_with_offsets("topic", 0 => Time.at(1505069646, 250_000))
      end

      tpl = list.to_native_tpl

      compare_list = described_class.new.tap do |list|
        list.add_topic_and_partitions_with_offsets(
          "topic",
          0 => (Time.at(1505069646, 250_000).to_f * 1000).floor
        )
      end

      native_list = described_class.from_native_tpl(tpl)

      assert_equal(compare_list, native_list)
    end

    it "destroys the native list when population fails partway" do
      # Keep a handle on the real destroy so the native list can still be released once the
      # stubbed call has been verified
      real_destroy = Rdkafka::Bindings.method(:rd_kafka_topic_partition_list_destroy)
      created = Rdkafka::Bindings.rd_kafka_topic_partition_list_new(1)

      Rdkafka::Bindings.stubs(:rd_kafka_topic_partition_list_new).returns(created)
      Rdkafka::Bindings
        .expects(:rd_kafka_topic_partition_list_destroy)
        .with { |tpl| tpl.address == created.address }

      list = described_class.new
      # A non-integer offset makes set_offset raise after the native list was already allocated.
      list.add_topic_and_partitions_with_offsets("topic", 0 => "not-an-offset")

      assert_raises(TypeError) { list.to_native_tpl }
    ensure
      real_destroy&.call(created) if created
    end
  end
end
