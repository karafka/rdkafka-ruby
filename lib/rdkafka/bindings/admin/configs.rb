# frozen_string_literal: true

module Rdkafka
  module Bindings
    attach_function :rd_kafka_DescribeConfigs, [:pointer, :pointer, :size_t, :pointer, :pointer], :void, blocking: true
    attach_function :rd_kafka_ConfigResource_new, [:int32, :pointer], :pointer
    attach_function :rd_kafka_ConfigResource_destroy, [:pointer], :void
    attach_function :rd_kafka_ConfigResource_destroy_array, [:pointer, :int32], :void
    attach_function :rd_kafka_event_DescribeConfigs_result, [:pointer], :pointer
    attach_function :rd_kafka_DescribeConfigs_result_resources, [:pointer, :pointer], :pointer
    attach_function :rd_kafka_ConfigResource_configs, [:pointer, :pointer], :pointer
    attach_function :rd_kafka_ConfigEntry_name, [:pointer], :string
    attach_function :rd_kafka_ConfigEntry_value, [:pointer], :string
    attach_function :rd_kafka_ConfigEntry_is_read_only, [:pointer], :int
    attach_function :rd_kafka_ConfigEntry_is_default, [:pointer], :int
    attach_function :rd_kafka_ConfigEntry_is_sensitive, [:pointer], :int
    attach_function :rd_kafka_ConfigEntry_is_synonym, [:pointer], :int
    attach_function :rd_kafka_ConfigEntry_synonyms, [:pointer, :pointer], :pointer
    attach_function :rd_kafka_ConfigResource_error, [:pointer], :int
    attach_function :rd_kafka_ConfigResource_error_string, [:pointer], :string
    attach_function :rd_kafka_IncrementalAlterConfigs, [:pointer, :pointer, :size_t, :pointer, :pointer], :void, blocking: true
    attach_function :rd_kafka_IncrementalAlterConfigs_result_resources, [:pointer, :pointer], :pointer
    attach_function :rd_kafka_ConfigResource_add_incremental_config, [:pointer, :string, :int32, :string], :pointer
    attach_function :rd_kafka_event_IncrementalAlterConfigs_result, [:pointer], :pointer

    RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS = 5
    RD_KAFKA_EVENT_DESCRIBECONFIGS_RESULT = 104

    RD_KAFKA_ADMIN_OP_INCREMENTALALTERCONFIGS = 16
    RD_KAFKA_EVENT_INCREMENTALALTERCONFIGS_RESULT = 131072

    RD_KAFKA_ALTER_CONFIG_OP_TYPE_SET = 0
    RD_KAFKA_ALTER_CONFIG_OP_TYPE_DELETE = 1
    RD_KAFKA_ALTER_CONFIG_OP_TYPE_APPEND = 2
    RD_KAFKA_ALTER_CONFIG_OP_TYPE_SUBTRACT = 3
  end
end
