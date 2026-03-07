# frozen_string_literal: true

require "test_helper"

class PartitionTest < Minitest::Test
  def test_has_partition
    subject = Rdkafka::Consumer::Partition.new(1, 100, 0)

    assert_equal 1, subject.partition
  end

  def test_has_offset
    subject = Rdkafka::Consumer::Partition.new(1, 100, 0)

    assert_equal 100, subject.offset
  end

  def test_has_err_code
    subject = Rdkafka::Consumer::Partition.new(1, 100, 0)

    assert_equal 0, subject.err
  end

  def test_to_s
    subject = Rdkafka::Consumer::Partition.new(1, 100, 0)

    assert_equal "<Partition 1 offset=100>", subject.to_s
  end

  def test_inspect
    subject = Rdkafka::Consumer::Partition.new(1, 100, 0)

    assert_equal "<Partition 1 offset=100>", subject.to_s
  end

  def test_to_s_without_offset
    subject = Rdkafka::Consumer::Partition.new(1, nil, 0)

    assert_equal "<Partition 1>", subject.to_s
  end

  def test_to_s_with_err_code
    subject = Rdkafka::Consumer::Partition.new(1, 100, 1)

    assert_equal "<Partition 1 offset=100 err=1>", subject.to_s
  end

  def test_equals_same_content
    subject = Rdkafka::Consumer::Partition.new(1, 100, 0)

    assert_equal Rdkafka::Consumer::Partition.new(1, 100), subject
  end

  def test_not_equals_different_content
    subject = Rdkafka::Consumer::Partition.new(1, 100, 0)

    refute_equal Rdkafka::Consumer::Partition.new(2, 101), subject
  end
end
