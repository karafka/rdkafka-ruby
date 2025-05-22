# frozen_string_literal: true

module Rdkafka
  # Base error class.
  class BaseError < RuntimeError; end

  # Error returned by the underlying rdkafka library.
  class RdkafkaError < BaseError
    # Empty hash for details default allocation
    EMPTY_HASH = {}.freeze

    # The underlying raw error response
    # @return [Integer]
    attr_reader :rdkafka_response

    # Prefix to be used for human readable representation
    # @return [String]
    attr_reader :message_prefix

    # Error message sent by the broker
    # @return [String]
    attr_reader :broker_message

    # Optional details hash specific to a given error or empty hash if none or not supported
    # @return [Hash]
    attr_reader :details

    class << self
      def build_from_c(response_ptr, message_prefix = nil, broker_message: nil)
        code = Rdkafka::Bindings.rd_kafka_error_code(response_ptr)

        return false if code.zero?

        message = broker_message || Rdkafka::Bindings.rd_kafka_err2str(code)
        fatal = !Rdkafka::Bindings.rd_kafka_error_is_fatal(response_ptr).zero?
        retryable = !Rdkafka::Bindings.rd_kafka_error_is_retriable(response_ptr).zero?
        abortable = !Rdkafka::Bindings.rd_kafka_error_txn_requires_abort(response_ptr).zero?

        Rdkafka::Bindings.rd_kafka_error_destroy(response_ptr)

        new(
          code,
          message_prefix,
          broker_message: message,
          fatal: fatal,
          retryable: retryable,
          abortable: abortable
        )
      end

      def build(response_ptr_or_code, message_prefix = nil, broker_message: nil)
        case response_ptr_or_code
        when Integer
          return false if response_ptr_or_code.zero?

          new(response_ptr_or_code, message_prefix, broker_message: broker_message)
        when Bindings::Message
          return false if response_ptr_or_code[:err].zero?

          unless response_ptr_or_code[:payload].null?
            message_prefix ||= response_ptr_or_code[:payload].read_string(response_ptr_or_code[:len])
          end

          details = if response_ptr_or_code[:rkt].null?
                      EMPTY_HASH
                    else
                      {
                        partition: response_ptr_or_code[:partition],
                        offset: response_ptr_or_code[:offset],
                        topic: Bindings.rd_kafka_topic_name(response_ptr_or_code[:rkt])
                      }.freeze
                    end
          new(
            response_ptr_or_code[:err],
            message_prefix,
            broker_message: broker_message,
            details: details
          )
        else
          build_from_c(response_ptr_or_code, message_prefix)
        end
      end

      def validate!(response_ptr_or_code, message_prefix = nil, broker_message: nil)
        error = build(response_ptr_or_code, message_prefix, broker_message: broker_message)
        error ? raise(error) : false
      end
    end

    # @private
    def initialize(
      response,
      message_prefix=nil,
      broker_message: nil,
      fatal: false,
      retryable: false,
      abortable: false,
      details: EMPTY_HASH
    )
      raise TypeError.new("Response has to be an integer") unless response.is_a? Integer
      @rdkafka_response = response
      @message_prefix = message_prefix
      @broker_message = broker_message
      @fatal = fatal
      @retryable = retryable
      @abortable = abortable
      @details = details
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

      err_str = Rdkafka::Bindings.rd_kafka_err2str(@rdkafka_response)
      base = "#{message_prefix_part}#{err_str} (#{code})"

      return base if broker_message.nil?
      return base if broker_message.empty?

      "#{base}\n#{broker_message}"
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

    def fatal?
      @fatal
    end

    def retryable?
      @retryable
    end

    def abortable?
      @abortable
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

  # Error class for public consumer method calls on a closed consumer.
  class ClosedConsumerError < BaseError
    def initialize(method)
      super("Illegal call to #{method.to_s} on a closed consumer")
    end
  end

  # Error class for public producer method calls on a closed producer.
  class ClosedProducerError < BaseError
    def initialize(method)
      super("Illegal call to #{method.to_s} on a closed producer")
    end
  end

  # Error class for public consumer method calls on a closed admin.
  class ClosedAdminError < BaseError
    def initialize(method)
      super("Illegal call to #{method.to_s} on a closed admin")
    end
  end

  class ClosedInnerError < BaseError
    def initialize
      super("Illegal call to a closed inner librdkafka instance")
    end
  end
end
