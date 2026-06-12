# frozen_string_literal: true

module Rdkafka
  # Base error class.
  class BaseError < RuntimeError; end

  # Error returned by the underlying rdkafka library.
  class RdkafkaError < BaseError
    # Mapping of raw librdkafka error codes to their symbolized names, built once at load
    # time from librdkafka's own error descriptor table. `#code` runs on hot paths (flush
    # timeout checks, partition EOF detection, error comparisons), so the lookup needs to be
    # read-only and allocation-free instead of rebuilding the name string and symbol each
    # time. The table is frozen, making lookups safe across threads and Ruby runtimes.
    CODES = begin
      descs_ptr = FFI::MemoryPointer.new(:pointer)
      count_ptr = FFI::MemoryPointer.new(:size_t)

      Rdkafka::Bindings.rd_kafka_get_err_descs(descs_ptr, count_ptr)

      array_of_descs = FFI::Pointer.new(Rdkafka::Bindings::NativeErrorDesc, descs_ptr.read_pointer)

      codes = {}

      count_ptr.read(:size_t).times do |i|
        desc = Rdkafka::Bindings::NativeErrorDesc.new(array_of_descs[i])

        next if desc[:name].null?

        # The descriptor table provides the set of known codes while the cached values are
        # computed through rd_kafka_err2name, the exact same transformation `#code` used per
        # call. This keeps parity even for sentinel entries the two sources name differently.
        name = Rdkafka::Bindings.rd_kafka_err2name(desc[:code]).downcase
        codes[desc[:code]] = (name[0] == "_" ? name[1..] : name).to_sym
      end

      codes.freeze
    end

    private_constant :CODES

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

    # @private
    # @param response [Integer] the raw error response code from librdkafka
    # @param message_prefix [String, nil] optional prefix for error messages
    # @param broker_message [String, nil] optional error message from the broker
    # @param instance_name [String, nil] optional name of the rdkafka instance
    def initialize(response, message_prefix = nil, broker_message: nil, instance_name: nil)
      raise TypeError.new("Response has to be an integer") unless response.is_a? Integer
      @rdkafka_response = response
      @message_prefix = message_prefix
      @broker_message = broker_message
      @instance_name = instance_name
    end

    # This error's code, for example `:partition_eof`, `:msg_size_too_large`.
    # @return [Symbol]
    def code
      # Fallback to the per-call computation for codes unknown to the descriptor table
      # (e.g. invalid response values), which librdkafka names dynamically
      CODES[@rdkafka_response] || begin
        code = Rdkafka::Bindings.rd_kafka_err2name(@rdkafka_response).downcase
        if code[0] == "_"
          code[1..].to_sym
        else
          code.to_sym
        end
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
