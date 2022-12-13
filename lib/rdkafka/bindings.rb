# frozen_string_literal: true

require "ffi"
require "json"
require "logger"

module Rdkafka
  # @private
  module Bindings
    extend FFI::Library

    def self.lib_extension
      if RbConfig::CONFIG['host_os'] =~ /darwin/
        'dylib'
      else
        'so'
      end
    end

    ffi_lib File.join(__dir__, "../../ext/librdkafka.#{lib_extension}")

    RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS = -175
    RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS = -174
    RD_KAFKA_RESP_ERR__NOENT = -156
    RD_KAFKA_RESP_ERR_NO_ERROR = 0

    RD_KAFKA_OFFSET_END       = -1
    RD_KAFKA_OFFSET_BEGINNING = -2
    RD_KAFKA_OFFSET_STORED    = -1000
    RD_KAFKA_OFFSET_INVALID   = -1001

    class SizePtr < FFI::Struct
      layout :value, :size_t
    end

    # Polling

    attach_function :rd_kafka_flush, [:pointer, :int], :void, blocking: true
    attach_function :rd_kafka_poll, [:pointer, :int], :void, blocking: true
    attach_function :rd_kafka_outq_len, [:pointer], :int, blocking: true

    # Metadata

    attach_function :rd_kafka_memberid, [:pointer], :string, blocking: true
    attach_function :rd_kafka_clusterid, [:pointer], :string, blocking: true
    attach_function :rd_kafka_metadata, [:pointer, :int, :pointer, :pointer, :int], :int, blocking: true
    attach_function :rd_kafka_metadata_destroy, [:pointer], :void

    # Message struct

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

    attach_function :rd_kafka_message_destroy, [:pointer], :void
    attach_function :rd_kafka_message_timestamp, [:pointer, :pointer], :int64
    attach_function :rd_kafka_topic_new, [:pointer, :string, :pointer], :pointer
    attach_function :rd_kafka_topic_destroy, [:pointer], :pointer
    attach_function :rd_kafka_topic_name, [:pointer], :string

    # TopicPartition ad TopicPartitionList structs

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

    class TopicPartitionList < FFI::Struct
      layout :cnt, :int,
             :size, :int,
             :elems, :pointer
    end

    attach_function :rd_kafka_topic_partition_list_new, [:int32], :pointer
    attach_function :rd_kafka_topic_partition_list_add, [:pointer, :string, :int32], :void
    attach_function :rd_kafka_topic_partition_list_set_offset, [:pointer, :string, :int32, :int64], :void
    attach_function :rd_kafka_topic_partition_list_destroy, [:pointer], :void
    attach_function :rd_kafka_topic_partition_list_copy, [:pointer], :pointer

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
    attach_function :rd_kafka_conf_set_opaque, [:pointer, :pointer], :void
    callback :stats_cb, [:pointer, :string, :int, :pointer], :int
    attach_function :rd_kafka_conf_set_stats_cb, [:pointer, :stats_cb], :void
    callback :error_cb, [:pointer, :int, :string, :pointer], :void
    attach_function :rd_kafka_conf_set_error_cb, [:pointer, :error_cb], :void
    attach_function :rd_kafka_rebalance_protocol, [:pointer], :string

    # Log queue
    attach_function :rd_kafka_set_log_queue, [:pointer, :pointer], :void
    attach_function :rd_kafka_queue_get_main, [:pointer], :pointer

    LogCallback = FFI::Function.new(
      :void, [:pointer, :int, :string, :string]
    ) do |_client_ptr, level, _level_string, line|
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
      Rdkafka::Config.log_queue << [severity, "rdkafka: #{line}"]
    end

    StatsCallback = FFI::Function.new(
      :int, [:pointer, :string, :int, :pointer]
    ) do |_client_ptr, json, _json_len, _opaque|
      # Pass the stats hash to callback in config
      if Rdkafka::Config.statistics_callback
        stats = JSON.parse(json)
        Rdkafka::Config.statistics_callback.call(stats)
      end

      # Return 0 so librdkafka frees the json string
      0
    end

    ErrorCallback = FFI::Function.new(
      :void, [:pointer, :int, :string, :pointer]
    ) do |_client_prr, err_code, reason, _opaque|
      if Rdkafka::Config.error_callback
        error = Rdkafka::RdkafkaError.new(err_code, broker_message: reason)
        Rdkafka::Config.error_callback.call(error)
      end
    end

    # Handle

    enum :kafka_type, [
      :rd_kafka_producer,
      :rd_kafka_consumer
    ]

    attach_function :rd_kafka_new, [:kafka_type, :pointer, :pointer, :int], :pointer

    attach_function :rd_kafka_destroy, [:pointer], :void

    # Consumer

    attach_function :rd_kafka_subscribe, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_unsubscribe, [:pointer], :int, blocking: true
    attach_function :rd_kafka_subscription, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_assign, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_incremental_assign, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_incremental_unassign, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_assignment, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_committed, [:pointer, :pointer, :int], :int, blocking: true
    attach_function :rd_kafka_commit, [:pointer, :pointer, :bool], :int, blocking: true
    attach_function :rd_kafka_poll_set_consumer, [:pointer], :void, blocking: true
    attach_function :rd_kafka_consumer_poll, [:pointer, :int], :pointer, blocking: true
    attach_function :rd_kafka_consumer_close, [:pointer], :void, blocking: true
    attach_function :rd_kafka_offset_store, [:pointer, :int32, :int64], :int, blocking: true
    attach_function :rd_kafka_pause_partitions, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_resume_partitions, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_seek, [:pointer, :int32, :int64, :int], :int, blocking: true

    # Headers
    attach_function :rd_kafka_header_get_all, [:pointer, :size_t, :pointer, :pointer, SizePtr], :int
    attach_function :rd_kafka_message_headers, [:pointer, :pointer], :int

    # Rebalance

    callback :rebalance_cb_function, [:pointer, :int, :pointer, :pointer], :void
    attach_function :rd_kafka_conf_set_rebalance_cb, [:pointer, :rebalance_cb_function], :void, blocking: true

    RebalanceCallback = FFI::Function.new(
      :void, [:pointer, :int, :pointer, :pointer]
    ) do |client_ptr, code, partitions_ptr, opaque_ptr|
      case code
      when RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS
        if Rdkafka::Bindings.rd_kafka_rebalance_protocol(client_ptr) == "COOPERATIVE"
          Rdkafka::Bindings.rd_kafka_incremental_assign(client_ptr, partitions_ptr)
        else
          Rdkafka::Bindings.rd_kafka_assign(client_ptr, partitions_ptr)
        end
      else # RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS or errors
        if Rdkafka::Bindings.rd_kafka_rebalance_protocol(client_ptr) == "COOPERATIVE"
          Rdkafka::Bindings.rd_kafka_incremental_unassign(client_ptr, partitions_ptr)
        else
          Rdkafka::Bindings.rd_kafka_assign(client_ptr, FFI::Pointer::NULL)
        end
      end

      opaque = Rdkafka::Config.opaques[opaque_ptr.to_i]
      return unless opaque

      tpl = Rdkafka::Consumer::TopicPartitionList.from_native_tpl(partitions_ptr).freeze
      consumer = Rdkafka::Consumer.new(client_ptr)

      begin
        case code
        when RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS
          opaque.call_on_partitions_assigned(consumer, tpl)
        when RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS
          opaque.call_on_partitions_revoked(consumer, tpl)
        end
      rescue Exception => err
        Rdkafka::Config.logger.error("Unhandled exception: #{err.class} - #{err.message}")
      end
    end

    # Stats

    attach_function :rd_kafka_query_watermark_offsets, [:pointer, :string, :int, :pointer, :pointer, :int], :int, blocking: true

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
    RD_KAFKA_VTYPE_HEADER = 9
    RD_KAFKA_VTYPE_HEADERS = 10

    RD_KAFKA_MSG_F_COPY = 0x2

    attach_function :rd_kafka_producev, [:pointer, :varargs], :int, blocking: true
    callback :delivery_cb, [:pointer, :pointer, :pointer], :void
    attach_function :rd_kafka_conf_set_dr_msg_cb, [:pointer, :delivery_cb], :void

    # Partitioner
    PARTITIONERS = %w(random consistent consistent_random murmur2 murmur2_random fnv1a fnv1a_random).each_with_object({}) do |name, hsh|
      method_name = "rd_kafka_msg_partitioner_#{name}".to_sym
      attach_function method_name, [:pointer, :pointer, :size_t, :int32, :pointer, :pointer], :int32
      hsh[name] = method_name
    end

    def self.partitioner(str, partition_count, partitioner_name = "consistent_random")
      # Return RD_KAFKA_PARTITION_UA(unassigned partition) when partition count is nil/zero.
      return -1 unless partition_count&.nonzero?

      str_ptr = str.empty? ? FFI::MemoryPointer::NULL : FFI::MemoryPointer.from_string(str)
      method_name = PARTITIONERS.fetch(partitioner_name) do
        raise Rdkafka::Config::ConfigError.new("Unknown partitioner: #{partitioner_name}")
      end
      public_send(method_name, nil, str_ptr, str.size > 0 ? str.size : 1, partition_count, nil, nil)
    end

    # Create Topics

    RD_KAFKA_ADMIN_OP_CREATETOPICS     = 1   # rd_kafka_admin_op_t
    RD_KAFKA_EVENT_CREATETOPICS_RESULT = 100 # rd_kafka_event_type_t

    attach_function :rd_kafka_CreateTopics, [:pointer, :pointer, :size_t, :pointer, :pointer], :void, blocking: true
    attach_function :rd_kafka_NewTopic_new, [:pointer, :size_t, :size_t, :pointer, :size_t], :pointer, blocking: true
    attach_function :rd_kafka_NewTopic_set_config, [:pointer, :string, :string], :int32, blocking: true
    attach_function :rd_kafka_NewTopic_destroy, [:pointer], :void, blocking: true
    attach_function :rd_kafka_event_CreateTopics_result, [:pointer], :pointer, blocking: true
    attach_function :rd_kafka_CreateTopics_result_topics, [:pointer, :pointer], :pointer, blocking: true

    # Delete Topics

    RD_KAFKA_ADMIN_OP_DELETETOPICS     = 2   # rd_kafka_admin_op_t
    RD_KAFKA_EVENT_DELETETOPICS_RESULT = 101 # rd_kafka_event_type_t

    attach_function :rd_kafka_DeleteTopics, [:pointer, :pointer, :size_t, :pointer, :pointer], :int32, blocking: true
    attach_function :rd_kafka_DeleteTopic_new, [:pointer], :pointer, blocking: true
    attach_function :rd_kafka_DeleteTopic_destroy, [:pointer], :void, blocking: true
    attach_function :rd_kafka_event_DeleteTopics_result, [:pointer], :pointer, blocking: true
    attach_function :rd_kafka_DeleteTopics_result_topics, [:pointer, :pointer], :pointer, blocking: true

    # Background Queue and Callback

    attach_function :rd_kafka_queue_get_background, [:pointer], :pointer
    attach_function :rd_kafka_conf_set_background_event_cb, [:pointer, :pointer], :void
    attach_function :rd_kafka_queue_destroy, [:pointer], :void

    # Admin Options

    attach_function :rd_kafka_AdminOptions_new, [:pointer, :int32], :pointer
    attach_function :rd_kafka_AdminOptions_set_opaque, [:pointer, :pointer], :void
    attach_function :rd_kafka_AdminOptions_destroy, [:pointer], :void

    # Extracting data from event types

    attach_function :rd_kafka_event_type, [:pointer], :int32
    attach_function :rd_kafka_event_opaque, [:pointer], :pointer

    # Extracting data from topic results

    attach_function :rd_kafka_topic_result_error, [:pointer], :int32
    attach_function :rd_kafka_topic_result_error_string, [:pointer], :pointer
    attach_function :rd_kafka_topic_result_name, [:pointer], :pointer
  end
end
