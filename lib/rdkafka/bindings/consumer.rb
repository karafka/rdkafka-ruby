# frozen_string_literal: true

module Rdkafka
  module Bindings
    # Consumer

    attach_function :rd_kafka_subscribe, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_unsubscribe, [:pointer], :int, blocking: true
    attach_function :rd_kafka_subscription, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_assign, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_incremental_assign, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_incremental_unassign, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_assignment, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_assignment_lost, [:pointer], :int, blocking: true
    attach_function :rd_kafka_committed, [:pointer, :pointer, :int], :int, blocking: true
    attach_function :rd_kafka_commit, [:pointer, :pointer, :bool], :int, blocking: true
    attach_function :rd_kafka_poll_set_consumer, [:pointer], :void, blocking: true
    attach_function :rd_kafka_consumer_poll, [:pointer, :int], :pointer, blocking: true
    # Non-blocking consumer poll variant (does not release GVL)
    # More efficient for poll(0) calls in fiber schedulers.
    attach_function :rd_kafka_consumer_poll_nb, :rd_kafka_consumer_poll, [:pointer, :int], :pointer, blocking: false
    attach_function :rd_kafka_consumer_close, [:pointer], :void, blocking: true
    attach_function :rd_kafka_queue_get_consumer, [:pointer], :pointer
    attach_function :rd_kafka_consume_batch_queue, [:pointer, :int, :pointer, :size_t], :ssize_t, blocking: true
    attach_function :rd_kafka_consume_batch_queue_nb, :rd_kafka_consume_batch_queue, [:pointer, :int, :pointer, :size_t], :ssize_t, blocking: false
    attach_function :rd_kafka_offsets_store, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_pause_partitions, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_resume_partitions, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_seek, [:pointer, :int32, :int64, :int], :int, blocking: true
    attach_function :rd_kafka_offsets_for_times, [:pointer, :pointer, :int], :int, blocking: true
    attach_function :rd_kafka_position, [:pointer, :pointer], :int, blocking: true
    # those two are used for eos support
    attach_function :rd_kafka_consumer_group_metadata, [:pointer], :pointer, blocking: true
    attach_function :rd_kafka_consumer_group_metadata_destroy, [:pointer], :void, blocking: true

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
      begin
        case code
        when RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS
          opaque.call_on_partitions_assigned(tpl)
        when RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS
          opaque.call_on_partitions_revoked(tpl)
        end
      rescue Exception => err
        Rdkafka::Config.logger.error("Unhandled exception: #{err.class} - #{err.message}")
      end
    end

    # Watermark offsets

    attach_function :rd_kafka_query_watermark_offsets, [:pointer, :string, :int, :pointer, :pointer, :int], :int, blocking: true
  end
end
