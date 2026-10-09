# frozen_string_literal: true

module Rdkafka
  module Bindings
    # List Offsets
    RD_KAFKA_ADMIN_OP_LISTOFFSETS = 21
    RD_KAFKA_EVENT_LISTOFFSETS_RESULT = 0x400000

    # rd_kafka_IsolationLevel_t
    RD_KAFKA_ISOLATION_LEVEL_READ_UNCOMMITTED = 0
    RD_KAFKA_ISOLATION_LEVEL_READ_COMMITTED = 1

    # rd_kafka_OffsetSpec_t
    RD_KAFKA_OFFSET_SPEC_MAX_TIMESTAMP = -3
    RD_KAFKA_OFFSET_SPEC_EARLIEST = -2
    RD_KAFKA_OFFSET_SPEC_LATEST = -1

    attach_function :rd_kafka_ListOffsets, [:pointer, :pointer, :pointer, :pointer], :void, blocking: true
    attach_function :rd_kafka_event_ListOffsets_result, [:pointer], :pointer
    attach_function :rd_kafka_ListOffsets_result_infos, [:pointer, :pointer], :pointer
    attach_function :rd_kafka_ListOffsetsResultInfo_topic_partition, [:pointer], :pointer
    attach_function :rd_kafka_ListOffsetsResultInfo_timestamp, [:pointer], :int64
    attach_function :rd_kafka_AdminOptions_set_isolation_level, [:pointer, :int], :pointer
    attach_function :rd_kafka_topic_partition_get_leader_epoch, [:pointer], :int32
  end
end
