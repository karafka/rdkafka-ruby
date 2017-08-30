module Rdkafka
  class RdkafkaError < RuntimeError
    attr_reader :rdkafka_response

    def initialize(response)
      raise TypeError.new("Response has to be an integer") unless response.is_a? Integer
      @rdkafka_response = response
    end

    def code
      code = Rdkafka::FFI.rd_kafka_err2name(@rdkafka_response).downcase
      if code[0] == "_"
        code[1..-1].to_sym
      else
        code.to_sym
      end
    end

    def to_s
      "#{Rdkafka::FFI.rd_kafka_err2str(@rdkafka_response)} (#{code})"
    end

    def is_partition_eof?
      code == :partition_eof
    end
  end
end
