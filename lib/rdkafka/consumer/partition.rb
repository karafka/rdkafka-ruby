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

      # @private
      def initialize(partition, offset)
        @partition = partition
        @offset = offset
      end

      # Human readable representation of this partition.
      # @return [String]
      def to_s
        if offset.nil?
          "<Partition #{partition} without offset>"
        else
          "<Partition #{partition} with offset #{offset}>"
        end
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
