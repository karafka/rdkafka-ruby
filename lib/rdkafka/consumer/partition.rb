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

      # @private
      def initialize(partition, offset, err = 0)
        @partition = partition
        @offset = offset
        @err = err
      end

      # Human readable representation of this partition.
      # @return [String]
      def to_s
        message = "<Partition #{partition}"
        message += " offset=#{offset}" if offset
        message += " err=#{err}" if err != 0
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
