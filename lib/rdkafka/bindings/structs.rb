# frozen_string_literal: true

module Rdkafka
  module Bindings
    # FFI struct representing a Kafka message (rd_kafka_message_t)
    class Message < FFI::Struct
      layout :err, :int,
        :rkt, :pointer,
        :partition, :int32,
        :payload, :pointer,
        :len, :size_t,
        :key, :pointer,
        :key_len, :size_t,
        :offset, :int64,
        :_private, :pointer
    end

    # FFI struct representing a topic partition (rd_kafka_topic_partition_t)
    class TopicPartition < FFI::Struct
      layout :topic, :string,
        :partition, :int32,
        :offset, :int64,
        :metadata, :pointer,
        :metadata_size, :size_t,
        :opaque, :pointer,
        :err, :int,
        :_private, :pointer
    end

    # FFI struct representing a topic partition list (rd_kafka_topic_partition_list_t)
    class TopicPartitionList < FFI::Struct
      layout :cnt, :int,
        :size, :int,
        :elems, :pointer
    end

    # FFI struct representing a config resource (rd_kafka_ConfigResource_t)
    # Structs for management of configurations. Each configuration is attached to a resource
    # and one resource can have many configuration details. Each resource will also have
    # separate errors results if obtaining configuration was not possible for any reason
    class ConfigResource < FFI::Struct
      layout :type, :int,
        :name, :string
    end

    # FFI struct for error description (rd_kafka_err_desc)
    class NativeErrorDesc < FFI::Struct
      layout :code, :int,
        :name, :pointer,
        :desc, :pointer
    end

    # FFI struct for native error (rd_kafka_error_t)
    class NativeError < FFI::Struct
      layout :code, :int32,
        :errstr, :pointer,
        :fatal, :u_int8_t,
        :retriable, :u_int8_t,
        :txn_requires_abort, :u_int8_t
    end
  end
end
