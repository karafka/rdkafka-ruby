require "ffi"
require "logger"

module Rdkafka
  module FFI
    extend ::FFI::Library

    def self.lib_extension
      if Gem::Platform.local.os.include?("darwin")
        'dylib'
      else
        'so'
      end
    end

    ffi_lib File.join(File.dirname(__FILE__), "../../ext/librdkafka.#{lib_extension}")

    # Polling

    attach_function :rd_kafka_poll, [:pointer, :int], :void

    # Message struct

    class Message < ::FFI::ManagedStruct
      layout :err, :int,
             :rkt, :pointer,
             :partition, :int32,
             :payload, :pointer,
             :len, :size_t,
             :key, :pointer,
             :key_len, :size_t,
             :offset, :int64,
             :_private, :pointer

      def err
        self[:err]
      end

      def topic
        FFI.rd_kafka_topic_name(self[:rkt])
      end

      def partition
        self[:partition]
      end

      def payload
        if self[:payload].null?
          nil
        else
          self[:payload].read_string(self[:len])
        end
      end

      def key
        if self[:key].null?
          nil
        else
          self[:key].read_string(self[:key_len])
        end
      end

      def offset
        self[:offset]
      end

      def to_s
        "Message in '#{topic}' with key '#{key}', payload '#{payload}', partition '#{partition}', offset '#{offset}'"
      end

      def self.release(ptr)
        rd_kafka_message_destroy(ptr)
      end
    end

    attach_function :rd_kafka_message_destroy, [:pointer], :void
    attach_function :rd_kafka_topic_name, [:pointer], :string

    # TopicPartition ad TopicPartitionList structs

    class TopicPartition < ::FFI::Struct
      layout :topic, :string,
             :partition, :int32,
             :offset, :int64,
             :metadata, :pointer,
             :metadata_size, :size_t,
             :opaque, :pointer,
             :err, :int,
             :_private, :pointer
    end

    class TopicPartitionList < ::FFI::Struct
      layout :cnt, :int,
             :size, :int,
             :elems, TopicPartition.ptr
    end

    attach_function :rd_kafka_topic_partition_list_new, [:int32], :pointer
    attach_function :rd_kafka_topic_partition_list_add, [:pointer, :string, :int32], :void
    attach_function :rd_kafka_topic_partition_list_destroy, [:pointer], :void

    # Errors

    attach_function :rd_kafka_err2name, [:int], :string
    attach_function :rd_kafka_err2str, [:int], :string

    # Configuration

    enum :kafka_config_response, [
      :config_unknown, -2,
      :config_invalid, -1,
      :config_ok, 0
    ]

    attach_function :rd_kafka_conf_new, [], :pointer
    attach_function :rd_kafka_conf_set, [:pointer, :string, :string, :pointer, :int], :kafka_config_response
    callback :log_cb, [:pointer, :int, :string, :string], :void
    attach_function :rd_kafka_conf_set_log_cb, [:pointer, :log_cb], :void

    LogCallback = Proc.new do |client_ptr, level, level_string, line|
      severity = case level
                 when 0 || 1 || 2
                   Logger::FATAL
                 when 3
                   Logger::ERROR
                 when 4
                   Logger::WARN
                 when 5 || 6
                   Logger::INFO
                 when 7
                   Logger::DEBUG
                 else
                   Logger::UNKNOWN
                 end
      Rdkafka::Config.logger.add(severity) { "rdkafka: #{line}" }
    end

    # Handle

    enum :kafka_type, [
      :rd_kafka_producer,
      :rd_kafka_consumer
    ]

    attach_function :rd_kafka_new, [:kafka_type, :pointer, :pointer, :int], :pointer
    attach_function :rd_kafka_destroy, [:pointer], :void

    # Consumer

    attach_function :rd_kafka_subscribe, [:pointer, :pointer], :int
    attach_function :rd_kafka_commit, [:pointer, :pointer, :bool], :int
    attach_function :rd_kafka_poll_set_consumer, [:pointer], :void
    attach_function :rd_kafka_consumer_poll, [:pointer, :int], :pointer

    # Producer

    RD_KAFKA_VTYPE_END = 0
    RD_KAFKA_VTYPE_TOPIC = 1
    RD_KAFKA_VTYPE_RKT = 2
    RD_KAFKA_VTYPE_PARTITION = 3
    RD_KAFKA_VTYPE_VALUE = 4
    RD_KAFKA_VTYPE_KEY = 5
    RD_KAFKA_VTYPE_OPAQUE = 6
    RD_KAFKA_VTYPE_MSGFLAGS = 7
    RD_KAFKA_VTYPE_TIMESTAMP = 8

    RD_KAFKA_MSG_F_COPY = 0x2

    attach_function :rd_kafka_producev, [:pointer, :varargs], :int
    callback :delivery_cb, [:pointer, :pointer, :pointer], :void
    attach_function :rd_kafka_conf_set_dr_msg_cb, [:pointer, :delivery_cb], :void

    DeliveryCallback = Proc.new do |client_ptr, message_ptr, opaque_ptr|
      message = Message.new(message_ptr)
      delivery_handle = Rdkafka::DeliveryHandle.new(message[:_private])
      delivery_handle[:pending] = false
      delivery_handle[:response] = message[:err]
      delivery_handle[:partition] = message[:partition]
      delivery_handle[:offset] = message[:offset]
    end
  end
end
