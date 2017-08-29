module Rdkafka
  class Consumer
    include Enumerable

    def initialize(native_kafka)
      @native_kafka = native_kafka
    end

    def subscribe(*topics)
      # Create topic partition list with topics and no partition set
      tpl = Rdkafka::FFI.rd_kafka_topic_partition_list_new(topics.length)
      topics.each do |topic|
        Rdkafka::FFI.rd_kafka_topic_partition_list_add(
          tpl,
          topic,
          -1
        )
      end
      # Subscribe to topic partition list and check this was successfull
      response = Rdkafka::FFI.rd_kafka_subscribe(@native_kafka, tpl)
      if response != 0
        raise Rdkafka::RdkafkaError.new(response)
      end
      # Clean up the topic partition list
      Rdkafka::FFI.rd_kafka_topic_partition_list_destroy(tpl)
    end

    def each(&block)
      loop do
        message_ptr = Rdkafka::FFI.rd_kafka_consumer_poll(@native_kafka, 10)
        if message_ptr
          message = Rdkafka::FFI::Message.new(message_ptr)
          if message[:err] != 0
            raise Rdkafka::RdkafkaError.new(message[:err])
          end
          block.call(message)
        else
          # Sleep here instead of using a longer poll timeout so interrupting the
          # program works properly, MRI has a  hard time interrupting FFI calls.
          sleep 0.1
          next
        end
      end
    end
  end
end
