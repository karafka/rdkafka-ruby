# frozen_string_literal: true

module Rdkafka
  # Base error class.
  class BaseError < RuntimeError; end

  # Error returned by the underlying rdkafka library.
  class RdkafkaError < BaseError
    # The underlying raw error response
    # @return [Integer]
    attr_reader :rdkafka_response

    # Prefix to be used for human readable representation
    # @return [String]
    attr_reader :message_prefix

    # Error message sent by the broker
    # @return [String]
    attr_reader :broker_message

    # The name of the rdkafka instance that generated this error
    # @return [String, nil]
    attr_reader :instance_name

    class << self
      # Builds an error from a rd_kafka_error_t pointer, as returned by the transactional
      # producer primitives (init_transactions, begin_transaction, commit_transaction,
      # abort_transaction, send_offsets_to_transaction).
      #
      # @param response_ptr [FFI::Pointer] pointer to a rd_kafka_error_t, or NULL on success
      # @param message_prefix [String, nil] optional prefix for the error message
      # @param instance_name [String, nil] optional name of the rdkafka instance
      # @return [RdkafkaError, false] error instance or false when the pointer indicates no error
      def build_from_c(response_ptr, message_prefix = nil, instance_name: nil)
        return false if response_ptr.null?

        code = Rdkafka::Bindings.rd_kafka_error_code(response_ptr)
        broker_message = Rdkafka::Bindings.rd_kafka_error_string(response_ptr).read_string
        fatal = !Rdkafka::Bindings.rd_kafka_error_is_fatal(response_ptr).zero?
        retryable = !Rdkafka::Bindings.rd_kafka_error_is_retriable(response_ptr).zero?
        abortable = !Rdkafka::Bindings.rd_kafka_error_txn_requires_abort(response_ptr).zero?

        Rdkafka::Bindings.rd_kafka_error_destroy(response_ptr)

        new(
          code,
          message_prefix,
          broker_message: broker_message,
          fatal: fatal,
          retryable: retryable,
          abortable: abortable,
          instance_name: instance_name
        )
      end
    end

    # @private
    # @param response [Integer] the raw error response code from librdkafka
    # @param message_prefix [String, nil] optional prefix for error messages
    # @param broker_message [String, nil] optional error message from the broker
    # @param fatal [Boolean] whether this error is fatal and the client is no longer usable
    # @param retryable [Boolean] whether the operation may succeed if retried
    # @param abortable [Boolean] whether this error requires the current transaction to be aborted
    # @param instance_name [String, nil] optional name of the rdkafka instance
    def initialize(response, message_prefix = nil, broker_message: nil, fatal: false, retryable: false, abortable: false, instance_name: nil)
      raise TypeError.new("Response has to be an integer") unless response.is_a? Integer
      @rdkafka_response = response
      @message_prefix = message_prefix
      @broker_message = broker_message
      @fatal = fatal
      @retryable = retryable
      @abortable = abortable
      @instance_name = instance_name
    end

    # This error's code, for example `:partition_eof`, `:msg_size_too_large`.
    # @return [Symbol]
    def code
      code = Rdkafka::Bindings.rd_kafka_err2name(@rdkafka_response).downcase
      if code[0] == "_"
        code[1..].to_sym
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
        ""
      end
      instance_name_part = if instance_name
        " [#{instance_name}]"
      else
        ""
      end
      "#{message_prefix_part}#{Rdkafka::Bindings.rd_kafka_err2str(@rdkafka_response)} (#{code})#{instance_name_part}"
    end

    # Whether this error indicates the partition is EOF.
    # @return [Boolean]
    def is_partition_eof?
      code == :partition_eof
    end

    # Whether this error is fatal and the client instance is no longer usable.
    # @return [Boolean]
    def fatal?
      @fatal
    end

    # Whether the operation may succeed if retried.
    # @return [Boolean]
    def retryable?
      @retryable
    end

    # Whether this error requires the current transaction to be aborted.
    # @return [Boolean]
    def abortable?
      @abortable
    end

    # Error comparison
    # @param other [Object] object to compare with
    # @return [Boolean]
    def ==(other)
      other.is_a?(self.class) && (to_s == other.to_s)
    end
  end

  # Error with topic partition list returned by the underlying rdkafka library.
  class RdkafkaTopicPartitionListError < RdkafkaError
    # @return [TopicPartitionList]
    attr_reader :topic_partition_list

    # @private
    # @param response [Integer] the raw error response code from librdkafka
    # @param topic_partition_list [TopicPartitionList] the topic partition list with error info
    # @param message_prefix [String, nil] optional prefix for error messages
    def initialize(response, topic_partition_list, message_prefix = nil)
      super(response, message_prefix)
      @topic_partition_list = topic_partition_list
    end
  end

  # Error class for public consumer method calls on a closed consumer.
  class ClosedConsumerError < BaseError
    # @param method [Symbol] the method that was called
    def initialize(method)
      super("Illegal call to #{method} on a closed consumer")
    end
  end

  # Error class for public producer method calls on a closed producer.
  class ClosedProducerError < BaseError
    # @param method [Symbol] the method that was called
    def initialize(method)
      super("Illegal call to #{method} on a closed producer")
    end
  end

  # Error class for public admin method calls on a closed admin.
  class ClosedAdminError < BaseError
    # @param method [Symbol] the method that was called
    def initialize(method)
      super("Illegal call to #{method} on a closed admin")
    end
  end

  # Error class for calls on a closed inner librdkafka instance.
  class ClosedInnerError < BaseError
    def initialize
      super("Illegal call to a closed inner librdkafka instance")
    end
  end

  # Error class for librdkafka library loading failures (e.g., glibc compatibility issues).
  class LibraryLoadError < BaseError; end
end
