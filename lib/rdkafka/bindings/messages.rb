# frozen_string_literal: true

module Rdkafka
  module Bindings
    # Messages, topics and topic partition lists

    attach_function :rd_kafka_message_destroy, [:pointer], :void
    attach_function :rd_kafka_message_timestamp, [:pointer, :pointer], :int64
    attach_function :rd_kafka_topic_new, [:pointer, :string, :pointer], :pointer
    attach_function :rd_kafka_topic_destroy, [:pointer], :pointer
    attach_function :rd_kafka_topic_name, [:pointer], :string

    attach_function :rd_kafka_topic_partition_list_new, [:int32], :pointer
    attach_function :rd_kafka_topic_partition_list_add, [:pointer, :string, :int32], :pointer
    attach_function :rd_kafka_topic_partition_list_set_offset, [:pointer, :string, :int32, :int64], :void
    attach_function :rd_kafka_topic_partition_list_destroy, [:pointer], :void
    attach_function :rd_kafka_topic_partition_list_copy, [:pointer], :pointer
  end
end
