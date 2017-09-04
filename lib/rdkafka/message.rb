module Rdkafka
  class Message
    attr_reader :topic, :partition, :payload, :key, :offset

    def initialize(native_message)
      unless native_message[:rkt].null?
        @topic = FFI.rd_kafka_topic_name(native_message[:rkt])
      end
      @partition = native_message[:partition]
      unless native_message[:payload].null?
        @payload = native_message[:payload].read_string(native_message[:len])
      end
      unless native_message[:key].null?
        @key = native_message[:key].read_string(native_message[:key_len])
      end
      @offset = native_message[:offset]
    end

    def to_s
      "Message in '#{topic}' with key '#{key}', payload '#{payload}', partition '#{partition}', offset '#{offset}'"
    end
  end
end
