module Rdkafka
  class Producer
    def initialize(native_kafka)
      @native_kafka = native_kafka
      # Start thread to poll client for delivery callbacks
      @thread = Thread.new do
        loop do
          Rdkafka::FFI.rd_kafka_poll(@native_kafka, 1000)
        end
      end.abort_on_exception = true
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

      delivery_handle = DeliveryHandle.new
      delivery_handle[:pending] = true
      delivery_handle[:response] = -1
      delivery_handle[:partition] = -1
      delivery_handle[:offset] = -1

      # Produce the message
      response = Rdkafka::FFI.rd_kafka_producev(
        @native_kafka,
        :int, Rdkafka::FFI::RD_KAFKA_VTYPE_TOPIC, :string, topic,
        :int, Rdkafka::FFI::RD_KAFKA_VTYPE_MSGFLAGS, :int, Rdkafka::FFI::RD_KAFKA_MSG_F_COPY,
        :int, Rdkafka::FFI::RD_KAFKA_VTYPE_VALUE, :buffer_in, payload, :size_t, payload_size,
        :int, Rdkafka::FFI::RD_KAFKA_VTYPE_KEY, :buffer_in, key, :size_t, key_size,
        :int, Rdkafka::FFI::RD_KAFKA_VTYPE_PARTITION, :int32, partition,
        :int, Rdkafka::FFI::RD_KAFKA_VTYPE_TIMESTAMP, :int64, timestamp,
        :int, Rdkafka::FFI::RD_KAFKA_VTYPE_OPAQUE, :pointer, delivery_handle,
        :int, Rdkafka::FFI::RD_KAFKA_VTYPE_END
      )

      # Raise error if the produce call was not successfull
      if response != 0
        raise RdkafkaError.new(response)
      end

      delivery_handle
    end
  end

  class WaitTimeoutError < RuntimeError; end

  class DeliveryHandle < ::FFI::Struct
    layout :pending, :bool,
           :response, :int,
           :partition, :int,
           :offset, :int64

    def pending?
      self[:pending]
    end

    # Wait for the delivery report
    def wait(timeout_in_seconds=10)
      timeout = if timeout_in_seconds
                  Time.now.to_i + timeout_in_seconds
                else
                  nil
                end
      loop do
        if pending?
          if timeout && timeout <= Time.now.to_i
            raise WaitTimeoutError.new("Waiting for delivery timed out after #{timeout_in_seconds} seconds")
          end
          sleep 0.1
          next
        elsif self[:response] != 0
          raise RdkafkaError.new(self[:response])
        else
          return DeliveryReport.new(self[:partition], self[:offset])
        end
      end
    end
  end

  class DeliveryReport
    attr_reader :partition, :offset

    def initialize(partition, offset)
      @partition = partition
      @offset = offset
    end
  end
end
