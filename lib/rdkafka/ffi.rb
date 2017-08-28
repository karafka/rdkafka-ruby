require "ffi"
require "mini_portile2"

module Rdkafka
  module FFI
    extend ::FFI::Library
    ffi_lib "ext/ports/#{MiniPortile.new("librdkafka", Rdkafka::LIBRDKAFKA_VERSION).host}/librdkafka/#{Rdkafka::LIBRDKAFKA_VERSION}/lib/librdkafka.dylib"

    # Polling
    attach_function :rd_kafka_poll, [:pointer, :int], :void

    # Message struct

    class Message < ::FFI::Struct
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

    # Handle

    enum :kafka_type, [
      :rd_kafka_producer,
      :rd_kafka_consumer
    ]

    attach_function :rd_kafka_new, [:kafka_type, :pointer, :pointer, :int], :pointer
    attach_function :rd_kafka_destroy, [:pointer], :void

    # Producing

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
