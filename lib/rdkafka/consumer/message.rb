module Rdkafka
  class Consumer
    # A message that was consumed from a topic.
    class Message
      # The topic this message was consumed from
      # @return [String]
      attr_reader :topic

      # The partition this message was consumed from
      # @return [Integer]
      attr_reader :partition

      # This message's payload
      # @return [String, nil]
      attr_reader :payload

      # This message's key
      # @return [String, nil]
      attr_reader :key

      # This message's offset in it's partition
      # @return [Integer]
      attr_reader :offset

      # This message's timestamp, if provided by the broker
      # @return [Integer, nil]
      attr_reader :timestamp

      # @private
      def initialize(native_message)
        unless native_message[:rkt].null?
          @topic = Rdkafka::Bindings.rd_kafka_topic_name(native_message[:rkt])
        end
        @partition = native_message[:partition]
        unless native_message[:payload].null?
          @payload = native_message[:payload].read_string(native_message[:len])
        end
        unless native_message[:key].null?
          @key = native_message[:key].read_string(native_message[:key_len])
        end
        @offset = native_message[:offset]
        @timestamp = Rdkafka::Bindings.rd_kafka_message_timestamp(native_message, nil)
      end

      # Human readable representation of this message.
      # @return [String]
      def to_s
        "<Message in '#{topic}' with key '#{truncate(key)}', payload '#{truncate(payload)}', partition #{partition}, offset #{offset}, timestamp #{timestamp}>"
      end

      def truncate(string)
        if string && string.length > 40
          "#{string[0..39]}..."
        else
          string
        end
      end
    end
  end
end
