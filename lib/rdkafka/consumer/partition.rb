# frozen_string_literal: true

module Rdkafka
  class Consumer
    # Information about a partition, used in {TopicPartitionList}.
    class Partition
      # Partition number
      # @return [Integer]
      attr_reader :partition

      # Partition's offset
      # @return [Integer, nil]
      attr_reader :offset

      # Partition's error code
      # @return [Integer]
      attr_reader :err

      # Partition metadata in the context of a consumer
      # @return [String, nil]
      attr_reader :metadata

      # @private
      def initialize(partition, offset, err = 0, metadata = nil)
        @partition = partition
        @offset = offset
        @err = err
        @metadata = metadata
      end

      # Human readable representation of this partition.
      # @return [String]
      def to_s
        message = "<Partition #{partition}"
        message += " offset=#{offset}" if offset
        message += " err=#{err}" if err != 0
        message += " metadata=#{metadata}" if metadata != nil
        message += ">"
        message
      end

      # Human readable representation of this partition.
      # @return [String]
      def inspect
        to_s
      end

      # Whether another partition is equal to this
      # @return [Boolean]
      def ==(other)
        self.class == other.class &&
          self.partition == other.partition &&
          self.offset == other.offset
      end
    end
  end
end
