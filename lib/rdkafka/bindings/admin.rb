# frozen_string_literal: true

module Rdkafka
  module Bindings
    # Create Topics
    RD_KAFKA_ADMIN_OP_CREATETOPICS = 1   # rd_kafka_admin_op_t
    RD_KAFKA_EVENT_CREATETOPICS_RESULT = 100 # rd_kafka_event_type_t

    attach_function :rd_kafka_CreateTopics, [:pointer, :pointer, :size_t, :pointer, :pointer], :void, blocking: true
    attach_function :rd_kafka_NewTopic_new, [:pointer, :size_t, :size_t, :pointer, :size_t], :pointer, blocking: true
    attach_function :rd_kafka_NewTopic_set_config, [:pointer, :string, :string], :int32, blocking: true
    attach_function :rd_kafka_NewTopic_destroy, [:pointer], :void, blocking: true
    attach_function :rd_kafka_event_CreateTopics_result, [:pointer], :pointer, blocking: true
    attach_function :rd_kafka_CreateTopics_result_topics, [:pointer, :pointer], :pointer, blocking: true

    # Delete Topics
    RD_KAFKA_ADMIN_OP_DELETETOPICS = 2   # rd_kafka_admin_op_t
    RD_KAFKA_EVENT_DELETETOPICS_RESULT = 101 # rd_kafka_event_type_t

    attach_function :rd_kafka_DeleteTopics, [:pointer, :pointer, :size_t, :pointer, :pointer], :int32, blocking: true
    attach_function :rd_kafka_DeleteTopic_new, [:pointer], :pointer, blocking: true
    attach_function :rd_kafka_DeleteTopic_destroy, [:pointer], :void, blocking: true
    attach_function :rd_kafka_event_DeleteTopics_result, [:pointer], :pointer, blocking: true
    attach_function :rd_kafka_DeleteTopics_result_topics, [:pointer, :pointer], :pointer, blocking: true

    # Create partitions
    RD_KAFKA_ADMIN_OP_CREATEPARTITIONS = 3
    RD_KAFKA_ADMIN_OP_CREATEPARTITIONS_RESULT = 102

    attach_function :rd_kafka_CreatePartitions, [:pointer, :pointer, :size_t, :pointer, :pointer], :void
    attach_function :rd_kafka_NewPartitions_new, %i[pointer size_t pointer size_t], :pointer
    attach_function :rd_kafka_NewPartitions_destroy, [:pointer], :void
    attach_function :rd_kafka_event_CreatePartitions_result, [:pointer], :pointer
    attach_function :rd_kafka_CreatePartitions_result_topics, [:pointer, :pointer], :pointer

    # Delete Group
    RD_KAFKA_ADMIN_OP_DELETEGROUPS = 7   # rd_kafka_admin_op_t
    RD_KAFKA_EVENT_DELETEGROUPS_RESULT = 106 # rd_kafka_event_type_t

    attach_function :rd_kafka_DeleteGroups, [:pointer, :pointer, :size_t, :pointer, :pointer], :void, blocking: true
    attach_function :rd_kafka_DeleteGroup_new, [:pointer], :pointer, blocking: true
    attach_function :rd_kafka_DeleteGroup_destroy, [:pointer], :void, blocking: true
    attach_function :rd_kafka_event_DeleteGroups_result, [:pointer], :pointer, blocking: true # rd_kafka_event_t* => rd_kafka_DeleteGroups_result_t*
    attach_function :rd_kafka_DeleteGroups_result_groups, [:pointer, :pointer], :pointer, blocking: true # rd_kafka_DeleteGroups_result_t*, size_t* => rd_kafka_group_result_t**

    # Alter Consumer Group Offsets
    RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS = 15   # rd_kafka_admin_op_t
    RD_KAFKA_EVENT_ALTERCONSUMERGROUPOFFSETS_RESULT = 0x10000 # rd_kafka_event_type_t

    attach_function :rd_kafka_AlterConsumerGroupOffsets, [:pointer, :pointer, :size_t, :pointer, :pointer], :void, blocking: true
    attach_function :rd_kafka_AlterConsumerGroupOffsets_new, [:pointer, :pointer], :pointer, blocking: true # const char*, const rd_kafka_topic_partition_list_t* => rd_kafka_AlterConsumerGroupOffsets_t*
    attach_function :rd_kafka_AlterConsumerGroupOffsets_destroy, [:pointer], :void, blocking: true
    attach_function :rd_kafka_event_AlterConsumerGroupOffsets_result, [:pointer], :pointer, blocking: true # rd_kafka_event_t* => rd_kafka_AlterConsumerGroupOffsets_result_t*
    attach_function :rd_kafka_AlterConsumerGroupOffsets_result_groups, [:pointer, :pointer], :pointer, blocking: true # rd_kafka_AlterConsumerGroupOffsets_result_t*, size_t* => rd_kafka_group_result_t**

    # Delete Consumer Group Offsets
    RD_KAFKA_ADMIN_OP_DELETECONSUMERGROUPOFFSETS = 8   # rd_kafka_admin_op_t
    RD_KAFKA_EVENT_DELETECONSUMERGROUPOFFSETS_RESULT = 107 # rd_kafka_event_type_t

    attach_function :rd_kafka_DeleteConsumerGroupOffsets, [:pointer, :pointer, :size_t, :pointer, :pointer], :void, blocking: true
    attach_function :rd_kafka_DeleteConsumerGroupOffsets_new, [:pointer, :pointer], :pointer, blocking: true # const char*, const rd_kafka_topic_partition_list_t* => rd_kafka_DeleteConsumerGroupOffsets_t*
    attach_function :rd_kafka_DeleteConsumerGroupOffsets_destroy, [:pointer], :void, blocking: true
    attach_function :rd_kafka_event_DeleteConsumerGroupOffsets_result, [:pointer], :pointer, blocking: true # rd_kafka_event_t* => rd_kafka_DeleteConsumerGroupOffsets_result_t*
    attach_function :rd_kafka_DeleteConsumerGroupOffsets_result_groups, [:pointer, :pointer], :pointer, blocking: true # rd_kafka_DeleteConsumerGroupOffsets_result_t*, size_t* => rd_kafka_group_result_t**

    # Delete Records
    RD_KAFKA_ADMIN_OP_DELETERECORDS = 6   # rd_kafka_admin_op_t
    RD_KAFKA_EVENT_DELETERECORDS_RESULT = 105 # rd_kafka_event_type_t

    attach_function :rd_kafka_DeleteRecords, [:pointer, :pointer, :size_t, :pointer, :pointer], :void, blocking: true
    attach_function :rd_kafka_DeleteRecords_new, [:pointer], :pointer, blocking: true
    attach_function :rd_kafka_DeleteRecords_destroy, [:pointer], :void, blocking: true
    attach_function :rd_kafka_event_DeleteRecords_result, [:pointer], :pointer, blocking: true # rd_kafka_event_t* => rd_kafka_DeleteRecords_result_t*
    attach_function :rd_kafka_DeleteRecords_result_offsets, [:pointer], :pointer, blocking: true # rd_kafka_DeleteRecords_result_t* => rd_kafka_topic_partition_list_t*

    # List Consumer Groups
    RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS = 12   # rd_kafka_admin_op_t
    RD_KAFKA_EVENT_LISTCONSUMERGROUPS_RESULT = 0x2000 # rd_kafka_event_type_t

    # Consumer group states (rd_kafka_consumer_group_state_t)
    RD_KAFKA_CONSUMER_GROUP_STATE_UNKNOWN = 0
    RD_KAFKA_CONSUMER_GROUP_STATE_PREPARING_REBALANCE = 1
    RD_KAFKA_CONSUMER_GROUP_STATE_COMPLETING_REBALANCE = 2
    RD_KAFKA_CONSUMER_GROUP_STATE_STABLE = 3
    RD_KAFKA_CONSUMER_GROUP_STATE_DEAD = 4
    RD_KAFKA_CONSUMER_GROUP_STATE_EMPTY = 5

    attach_function :rd_kafka_ListConsumerGroups, [:pointer, :pointer, :pointer], :void, blocking: true
    attach_function :rd_kafka_event_ListConsumerGroups_result, [:pointer], :pointer, blocking: true # rd_kafka_event_t* => rd_kafka_ListConsumerGroups_result_t*
    attach_function :rd_kafka_ListConsumerGroups_result_valid, [:pointer, :pointer], :pointer, blocking: true # result*, size_t* => rd_kafka_ConsumerGroupListing_t**
    attach_function :rd_kafka_ListConsumerGroups_result_errors, [:pointer, :pointer], :pointer, blocking: true # result*, size_t* => rd_kafka_error_t**
    attach_function :rd_kafka_ConsumerGroupListing_group_id, [:pointer], :pointer, blocking: true # => const char*
    attach_function :rd_kafka_ConsumerGroupListing_is_simple_consumer_group, [:pointer], :int, blocking: true
    attach_function :rd_kafka_ConsumerGroupListing_state, [:pointer], :int, blocking: true
    attach_function :rd_kafka_consumer_group_state_name, [:int], :pointer, blocking: true # => const char*

    # Describe Consumer Groups
    RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS = 13   # rd_kafka_admin_op_t
    RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT = 0x4000 # rd_kafka_event_type_t

    # Consumer group types (rd_kafka_consumer_group_type_t)
    RD_KAFKA_CONSUMER_GROUP_TYPE_UNKNOWN = 0
    RD_KAFKA_CONSUMER_GROUP_TYPE_CONSUMER = 1
    RD_KAFKA_CONSUMER_GROUP_TYPE_CLASSIC = 2

    attach_function :rd_kafka_DescribeConsumerGroups, [:pointer, :pointer, :size_t, :pointer, :pointer], :void, blocking: true
    attach_function :rd_kafka_event_DescribeConsumerGroups_result, [:pointer], :pointer, blocking: true # rd_kafka_event_t* => rd_kafka_DescribeConsumerGroups_result_t*
    attach_function :rd_kafka_DescribeConsumerGroups_result_groups, [:pointer, :pointer], :pointer, blocking: true # result*, size_t* => rd_kafka_ConsumerGroupDescription_t**
    attach_function :rd_kafka_ConsumerGroupDescription_group_id, [:pointer], :pointer, blocking: true # => const char*
    attach_function :rd_kafka_ConsumerGroupDescription_error, [:pointer], :pointer, blocking: true # => const rd_kafka_error_t*
    attach_function :rd_kafka_ConsumerGroupDescription_is_simple_consumer_group, [:pointer], :int, blocking: true
    attach_function :rd_kafka_ConsumerGroupDescription_partition_assignor, [:pointer], :pointer, blocking: true # => const char*
    attach_function :rd_kafka_ConsumerGroupDescription_state, [:pointer], :int, blocking: true
    attach_function :rd_kafka_ConsumerGroupDescription_type, [:pointer], :int, blocking: true
    attach_function :rd_kafka_ConsumerGroupDescription_authorized_operations, [:pointer, :pointer], :pointer, blocking: true # grpdesc*, size_t* => const rd_kafka_AclOperation_t*
    attach_function :rd_kafka_ConsumerGroupDescription_coordinator, [:pointer], :pointer, blocking: true # => const rd_kafka_Node_t*
    attach_function :rd_kafka_ConsumerGroupDescription_member_count, [:pointer], :size_t, blocking: true
    attach_function :rd_kafka_ConsumerGroupDescription_member, [:pointer, :size_t], :pointer, blocking: true # => const rd_kafka_MemberDescription_t*
    attach_function :rd_kafka_MemberDescription_client_id, [:pointer], :pointer, blocking: true # => const char*
    attach_function :rd_kafka_MemberDescription_group_instance_id, [:pointer], :pointer, blocking: true # => const char*
    attach_function :rd_kafka_MemberDescription_consumer_id, [:pointer], :pointer, blocking: true # => const char*
    attach_function :rd_kafka_MemberDescription_host, [:pointer], :pointer, blocking: true # => const char*
    attach_function :rd_kafka_MemberDescription_assignment, [:pointer], :pointer, blocking: true # => const rd_kafka_MemberAssignment_t*
    attach_function :rd_kafka_MemberDescription_target_assignment, [:pointer], :pointer, blocking: true # => const rd_kafka_MemberAssignment_t*
    attach_function :rd_kafka_MemberAssignment_partitions, [:pointer], :pointer, blocking: true # => const rd_kafka_topic_partition_list_t*
    attach_function :rd_kafka_consumer_group_type_name, [:int], :pointer, blocking: true # => const char*

    # Describe Topics and Describe Cluster
    RD_KAFKA_ADMIN_OP_DESCRIBETOPICS = 19   # rd_kafka_admin_op_t
    RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT = 0x100000 # rd_kafka_event_type_t
    RD_KAFKA_ADMIN_OP_DESCRIBECLUSTER = 20   # rd_kafka_admin_op_t
    RD_KAFKA_EVENT_DESCRIBECLUSTER_RESULT = 0x200000 # rd_kafka_event_type_t

    attach_function :rd_kafka_AdminOptions_set_include_authorized_operations, [:pointer, :int], :pointer, blocking: true # => rd_kafka_error_t*
    attach_function :rd_kafka_TopicCollection_of_topic_names, [:pointer, :size_t], :pointer, blocking: true # const char**, size_t => rd_kafka_TopicCollection_t*
    attach_function :rd_kafka_TopicCollection_destroy, [:pointer], :void, blocking: true
    attach_function :rd_kafka_DescribeTopics, [:pointer, :pointer, :pointer, :pointer], :void, blocking: true
    attach_function :rd_kafka_event_DescribeTopics_result, [:pointer], :pointer, blocking: true # rd_kafka_event_t* => rd_kafka_DescribeTopics_result_t*
    attach_function :rd_kafka_DescribeTopics_result_topics, [:pointer, :pointer], :pointer, blocking: true # result*, size_t* => rd_kafka_TopicDescription_t**
    attach_function :rd_kafka_TopicDescription_name, [:pointer], :pointer, blocking: true # => const char*
    attach_function :rd_kafka_TopicDescription_topic_id, [:pointer], :pointer, blocking: true # => rd_kafka_Uuid_t*
    attach_function :rd_kafka_TopicDescription_is_internal, [:pointer], :int, blocking: true
    attach_function :rd_kafka_TopicDescription_partitions, [:pointer, :pointer], :pointer, blocking: true # desc*, size_t* => rd_kafka_TopicPartitionInfo_t**
    attach_function :rd_kafka_TopicDescription_authorized_operations, [:pointer, :pointer], :pointer, blocking: true # desc*, size_t* => rd_kafka_AclOperation_t*
    attach_function :rd_kafka_TopicDescription_error, [:pointer], :pointer, blocking: true # => rd_kafka_error_t*
    attach_function :rd_kafka_TopicPartitionInfo_partition, [:pointer], :int, blocking: true
    attach_function :rd_kafka_TopicPartitionInfo_leader, [:pointer], :pointer, blocking: true # => rd_kafka_Node_t*
    attach_function :rd_kafka_TopicPartitionInfo_replicas, [:pointer, :pointer], :pointer, blocking: true # info*, size_t* => rd_kafka_Node_t**
    attach_function :rd_kafka_TopicPartitionInfo_isr, [:pointer, :pointer], :pointer, blocking: true # info*, size_t* => rd_kafka_Node_t**
    attach_function :rd_kafka_Uuid_base64str, [:pointer], :pointer, blocking: true # => const char*
    attach_function :rd_kafka_Uuid_most_significant_bits, [:pointer], :int64, blocking: true
    attach_function :rd_kafka_Uuid_least_significant_bits, [:pointer], :int64, blocking: true
    attach_function :rd_kafka_DescribeCluster, [:pointer, :pointer, :pointer], :void, blocking: true
    attach_function :rd_kafka_event_DescribeCluster_result, [:pointer], :pointer, blocking: true # rd_kafka_event_t* => rd_kafka_DescribeCluster_result_t*
    attach_function :rd_kafka_DescribeCluster_result_nodes, [:pointer, :pointer], :pointer, blocking: true # result*, size_t* => rd_kafka_Node_t**
    attach_function :rd_kafka_DescribeCluster_result_controller, [:pointer], :pointer, blocking: true # => rd_kafka_Node_t*
    attach_function :rd_kafka_DescribeCluster_result_cluster_id, [:pointer], :pointer, blocking: true # => const char*
    attach_function :rd_kafka_DescribeCluster_result_authorized_operations, [:pointer, :pointer], :pointer, blocking: true # result*, size_t* => rd_kafka_AclOperation_t*
    attach_function :rd_kafka_Node_id, [:pointer], :int, blocking: true
    attach_function :rd_kafka_Node_host, [:pointer], :pointer, blocking: true # => const char*
    attach_function :rd_kafka_Node_port, [:pointer], :uint16, blocking: true
    attach_function :rd_kafka_Node_rack, [:pointer], :pointer, blocking: true # => const char*

    # Background Queue and Callback

    attach_function :rd_kafka_conf_set_background_event_cb, [:pointer, :pointer], :void
    attach_function :rd_kafka_queue_destroy, [:pointer], :void

    # Admin Options

    attach_function :rd_kafka_AdminOptions_new, [:pointer, :int32], :pointer
    attach_function :rd_kafka_AdminOptions_set_opaque, [:pointer, :pointer], :void
    attach_function :rd_kafka_AdminOptions_destroy, [:pointer], :void

    # Extracting data from event types

    attach_function :rd_kafka_event_type, [:pointer], :int32
    attach_function :rd_kafka_event_opaque, [:pointer], :pointer
    attach_function :rd_kafka_event_destroy, [:pointer], :void
    attach_function :rd_kafka_event_error, [:pointer], :int32
    attach_function :rd_kafka_event_error_string, [:pointer], :pointer

    # Extracting data from topic results

    attach_function :rd_kafka_topic_result_error, [:pointer], :int32
    attach_function :rd_kafka_topic_result_error_string, [:pointer], :pointer
    attach_function :rd_kafka_topic_result_name, [:pointer], :pointer

    attach_function :rd_kafka_group_result_error, [:pointer], NativeError.by_ref # rd_kafka_group_result_t* => rd_kafka_error_t*
    attach_function :rd_kafka_group_result_name, [:pointer], :pointer
    attach_function :rd_kafka_group_result_partitions, [:pointer], :pointer # rd_kafka_group_result_t* => rd_kafka_topic_partition_list_t*
  end
end

require_relative "admin/configs"
require_relative "admin/list_offsets"
require_relative "admin/acls"
