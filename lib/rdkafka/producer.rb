module Rdkafka
  class Producer
    def initialize(native_kafka)
      @native_kafka = native_kafka
    end

    def produce(topic:, payload: nil, key: nil, partition: nil, timestamp: nil)
      # Start by checking and converting the input

      # Get payload length
      payload_size = if payload.nil?
                       0
                     else
                       payload.bytesize
                     end

      # Get key length
      key_size = if key.nil?
                   0
                 else
                   key.bytesize
                 end

      # If partition is nil use -1 to let Kafka set the partition based
      # on the key/randomly if there is no key
      partition = -1 if partition.nil?

      # If timestamp is nil use 0 and let Kafka set one
      timestamp = 0 if timestamp.nil?

      # Produce the message
      response = Rdkafka::FFI.rd_kafka_producev(
        @native_kafka,
        :int, Rdkafka::FFI::RD_KAFKA_VTYPE_TOPIC, :string, topic,
        :int, Rdkafka::FFI::RD_KAFKA_VTYPE_MSGFLAGS, :int, Rdkafka::FFI::RD_KAFKA_MSG_F_COPY,
        :int, Rdkafka::FFI::RD_KAFKA_VTYPE_VALUE, :buffer_in, payload, :size_t, payload_size,
        :int, Rdkafka::FFI::RD_KAFKA_VTYPE_KEY, :buffer_in, key, :size_t, key_size,
        :int, Rdkafka::FFI::RD_KAFKA_VTYPE_PARTITION, :int, partition,
        #Rdkafka::FFI::RD_KAFKA_VTYPE_OPAQUE, delivery_context_ptr,
        :int, Rdkafka::FFI::RD_KAFKA_VTYPE_TIMESTAMP, :int, timestamp,
        :int, Rdkafka::FFI::RD_KAFKA_VTYPE_END
      )

      # Raise error if the produce call was not successfull
      if response != 0
        raise RdkafkaError.new(response)
      end

      # NEXT: Return delivery future
    end
  end
end
