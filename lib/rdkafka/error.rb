module Rdkafka
  # Base error class.
  class BaseError < RuntimeError; end

  # Error returned by the underlying rdkafka library.
  class RdkafkaError < BaseError
    # The underlying raw error response
    # @return [Integer]
    attr_reader :rdkafka_response, :message_prefix

    # @private
    def initialize(response, message_prefix=nil)
      raise TypeError.new("Response has to be an integer") unless response.is_a? Integer

      @rdkafka_response = response
      @message_prefix = message_prefix
    end

    # This error's code, for example `:partition_eof`, `:msg_size_too_large`.
    # @return [Symbol]
    def code
      code = Rdkafka::Bindings.rd_kafka_err2name(@rdkafka_response).downcase
      if code[0] == "_"
        code[1..-1].to_sym
      else
        code.to_sym
      end
    end

    # Human readable representation of this error.
    # @return [String]
    def to_s
      message_prefix_part = if message_prefix
                       "#{message_prefix} - "
                     else
                       ''
                     end
      "#{message_prefix_part}#{Rdkafka::Bindings.rd_kafka_err2str(@rdkafka_response)} (#{code})"
    end

    # Whether this error indicates the partition is EOF.
    # @return [Boolean]
    def is_partition_eof?
      code == :partition_eof
    end

    # Error comparison
    def ==(another_error)
       another_error.is_a?(self.class) && (self.to_s == another_error.to_s)
    end
  end

  # Error with topic partition list returned by the underlying rdkafka library.
  class RdkafkaTopicPartitionListError < RdkafkaError
    # @return [TopicPartitionList]
    attr_reader :topic_partition_list

    # @private
    def initialize(response, topic_partition_list, message_prefix=nil)
      super(response, message_prefix)
      @topic_partition_list = topic_partition_list
    end
  end

  # Consumer closed error.
  class ConsumerClosedError < BaseError; end

  # Producer closed error.
  class ProducerClosedError < BaseError; end

  # Error that is returned by the underlying rdkafka library if the client cannot be created.
  class ClientCreationError < RuntimeError; end
end
