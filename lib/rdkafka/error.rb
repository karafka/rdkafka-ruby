module Rdkafka
  # Error returned by the underlying rdkafka library.
  class RdkafkaError < RuntimeError
    # The underlying raw error response
    # @return [Integer]
    attr_reader :rdkafka_response

    # @private
    def initialize(response)
      raise TypeError.new("Response has to be an integer") unless response.is_a? Integer
      @rdkafka_response = response
    end

    # This error's code, for example `:partition_eof`, `:msg_size_too_large`.
    # @return [Symbol]
    def code
      code = Rdkafka::FFI.rd_kafka_err2name(@rdkafka_response).downcase
      if code[0] == "_"
        code[1..-1].to_sym
      else
        code.to_sym
      end
    end

    # Human readable representation of this error.
    # @return [String]
    def to_s
      "#{Rdkafka::FFI.rd_kafka_err2str(@rdkafka_response)} (#{code})"
    end

    # Whether this error indicates the partition is EOF.
    # @return [Boolean]
    def is_partition_eof?
      code == :partition_eof
    end
  end
end
