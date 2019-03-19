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
      # @return [Time, nil]
      attr_reader :timestamp

      # @return [Hash<String, String>] a message headers
      attr_reader :headers

      # @private
      def initialize(native_message)
        # Set topic
        unless native_message[:rkt].null?
          @topic = Rdkafka::Bindings.rd_kafka_topic_name(native_message[:rkt])
        end
        # Set partition
        @partition = native_message[:partition]
        # Set payload
        unless native_message[:payload].null?
          @payload = native_message[:payload].read_string(native_message[:len])
        end
        # Set key
        unless native_message[:key].null?
          @key = native_message[:key].read_string(native_message[:key_len])
        end
        # Set offset
        @offset = native_message[:offset]
        # Set timestamp
        raw_timestamp = Rdkafka::Bindings.rd_kafka_message_timestamp(native_message, nil)
        @timestamp = if raw_timestamp && raw_timestamp > -1
                       # Calculate seconds and microseconds
                       seconds = raw_timestamp / 1000
                       milliseconds = (raw_timestamp - seconds * 1000) * 1000
                       Time.at(seconds, milliseconds)
                     else
                       nil
                     end

        @headers = Headers.from_native(native_message)
      end

      # Human readable representation of this message.
      # @return [String]
      def to_s
        is_headers = @headers.empty? ? "" : ", headers #{headers.size}"

        "<Message in '#{topic}' with key '#{truncate(key)}', payload '#{truncate(payload)}', partition #{partition}, offset #{offset}, timestamp #{timestamp}#{is_headers}>"
      end

      def truncate(string)
        if string && string.length > 40
          "#{string[0..39]}..."
        else
          string
        end
      end

      private

    end
  end
end
