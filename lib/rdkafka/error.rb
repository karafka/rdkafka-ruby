module Rdkafka
  class RdkafkaError < RuntimeError
    attr_reader :rdkafka_response

    def initialize(response)
      @rdkafka_response = response
    end

    def code
      if @rdkafka_response.nil?
        :unknown_error
      else
        Rdkafka::FFI.rd_kafka_err2name(@rdkafka_response).downcase.to_sym
      end
    end

    def to_s
      if @rdkafka_response.nil?
        "Unknown error: Response code is nil"
      else
        Rdkafka::FFI.rd_kafka_err2str(@rdkafka_response)
      end
    end
  end
end
