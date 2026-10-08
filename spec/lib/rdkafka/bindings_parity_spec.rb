# frozen_string_literal: true

# Guards the public `Rdkafka::Bindings` surface defined across `bindings.rb` and
# `lib/rdkafka/bindings/*.rb`. Adding bindings needs no change here; removing or renaming one fails.
RSpec.describe Rdkafka::Bindings do
  let(:expected_methods) do
    %i[
      attach_function attach_variable attached_functions attached_variables bitmask callback enum
      enum_type enum_value ffi_convention ffi_lib ffi_lib_flags ffi_libraries find_type freeze
      function_names lib_extension partition_key_bytesize partition_key_length partition_key_size
      partitioner rd_kafka_AclBindingFilter_new rd_kafka_AclBinding_destroy
      rd_kafka_AclBinding_error rd_kafka_AclBinding_host rd_kafka_AclBinding_name
      rd_kafka_AclBinding_new rd_kafka_AclBinding_operation rd_kafka_AclBinding_permission_type
      rd_kafka_AclBinding_principal rd_kafka_AclBinding_resource_pattern_type
      rd_kafka_AclBinding_restype rd_kafka_AdminOptions_destroy rd_kafka_AdminOptions_new
      rd_kafka_AdminOptions_set_include_authorized_operations
      rd_kafka_AdminOptions_set_isolation_level rd_kafka_AdminOptions_set_opaque
      rd_kafka_AlterConsumerGroupOffsets rd_kafka_AlterConsumerGroupOffsets_destroy
      rd_kafka_AlterConsumerGroupOffsets_new rd_kafka_AlterConsumerGroupOffsets_result_groups
      rd_kafka_ConfigEntry_is_default rd_kafka_ConfigEntry_is_read_only
      rd_kafka_ConfigEntry_is_sensitive rd_kafka_ConfigEntry_is_synonym rd_kafka_ConfigEntry_name
      rd_kafka_ConfigEntry_synonyms rd_kafka_ConfigEntry_value
      rd_kafka_ConfigResource_add_incremental_config rd_kafka_ConfigResource_configs
      rd_kafka_ConfigResource_destroy rd_kafka_ConfigResource_destroy_array
      rd_kafka_ConfigResource_error rd_kafka_ConfigResource_error_string rd_kafka_ConfigResource_new
      rd_kafka_ConsumerGroupDescription_authorized_operations
      rd_kafka_ConsumerGroupDescription_coordinator rd_kafka_ConsumerGroupDescription_error
      rd_kafka_ConsumerGroupDescription_group_id
      rd_kafka_ConsumerGroupDescription_is_simple_consumer_group
      rd_kafka_ConsumerGroupDescription_member rd_kafka_ConsumerGroupDescription_member_count
      rd_kafka_ConsumerGroupDescription_partition_assignor rd_kafka_ConsumerGroupDescription_state
      rd_kafka_ConsumerGroupDescription_type rd_kafka_ConsumerGroupListing_group_id
      rd_kafka_ConsumerGroupListing_is_simple_consumer_group rd_kafka_ConsumerGroupListing_state
      rd_kafka_CreateAcls rd_kafka_CreateAcls_result_acls rd_kafka_CreatePartitions
      rd_kafka_CreatePartitions_result_topics rd_kafka_CreateTopics
      rd_kafka_CreateTopics_result_topics rd_kafka_DeleteAcls
      rd_kafka_DeleteAcls_result_response_error rd_kafka_DeleteAcls_result_response_matching_acls
      rd_kafka_DeleteAcls_result_responses rd_kafka_DeleteConsumerGroupOffsets
      rd_kafka_DeleteConsumerGroupOffsets_destroy rd_kafka_DeleteConsumerGroupOffsets_new
      rd_kafka_DeleteConsumerGroupOffsets_result_groups rd_kafka_DeleteGroup_destroy
      rd_kafka_DeleteGroup_new rd_kafka_DeleteGroups rd_kafka_DeleteGroups_result_groups
      rd_kafka_DeleteRecords rd_kafka_DeleteRecords_destroy rd_kafka_DeleteRecords_new
      rd_kafka_DeleteRecords_result_offsets rd_kafka_DeleteTopic_destroy rd_kafka_DeleteTopic_new
      rd_kafka_DeleteTopics rd_kafka_DeleteTopics_result_topics rd_kafka_DescribeAcls
      rd_kafka_DescribeAcls_result_acls rd_kafka_DescribeCluster
      rd_kafka_DescribeCluster_result_authorized_operations
      rd_kafka_DescribeCluster_result_cluster_id rd_kafka_DescribeCluster_result_controller
      rd_kafka_DescribeCluster_result_nodes rd_kafka_DescribeConfigs
      rd_kafka_DescribeConfigs_result_resources rd_kafka_DescribeConsumerGroups
      rd_kafka_DescribeConsumerGroups_result_groups rd_kafka_DescribeTopics
      rd_kafka_DescribeTopics_result_topics rd_kafka_IncrementalAlterConfigs
      rd_kafka_IncrementalAlterConfigs_result_resources rd_kafka_ListConsumerGroups
      rd_kafka_ListConsumerGroups_result_errors rd_kafka_ListConsumerGroups_result_valid
      rd_kafka_ListOffsets rd_kafka_ListOffsetsResultInfo_timestamp
      rd_kafka_ListOffsetsResultInfo_topic_partition rd_kafka_ListOffsets_result_infos
      rd_kafka_MemberAssignment_partitions rd_kafka_MemberDescription_assignment
      rd_kafka_MemberDescription_client_id rd_kafka_MemberDescription_consumer_id
      rd_kafka_MemberDescription_group_instance_id rd_kafka_MemberDescription_host
      rd_kafka_MemberDescription_target_assignment rd_kafka_NewPartitions_destroy
      rd_kafka_NewPartitions_new rd_kafka_NewTopic_destroy rd_kafka_NewTopic_new
      rd_kafka_NewTopic_set_config rd_kafka_Node_host rd_kafka_Node_id rd_kafka_Node_port
      rd_kafka_Node_rack rd_kafka_TopicCollection_destroy rd_kafka_TopicCollection_of_topic_names
      rd_kafka_TopicDescription_authorized_operations rd_kafka_TopicDescription_error
      rd_kafka_TopicDescription_is_internal rd_kafka_TopicDescription_name
      rd_kafka_TopicDescription_partitions rd_kafka_TopicDescription_topic_id
      rd_kafka_TopicPartitionInfo_isr rd_kafka_TopicPartitionInfo_leader
      rd_kafka_TopicPartitionInfo_partition rd_kafka_TopicPartitionInfo_replicas
      rd_kafka_Uuid_base64str rd_kafka_Uuid_least_significant_bits
      rd_kafka_Uuid_most_significant_bits rd_kafka_acl_result_error rd_kafka_assign
      rd_kafka_assignment rd_kafka_assignment_lost rd_kafka_clusterid rd_kafka_commit
      rd_kafka_committed rd_kafka_conf rd_kafka_conf_destroy rd_kafka_conf_dump
      rd_kafka_conf_dump_free rd_kafka_conf_get rd_kafka_conf_new rd_kafka_conf_set
      rd_kafka_conf_set_background_event_cb rd_kafka_conf_set_dr_msg_cb rd_kafka_conf_set_error_cb
      rd_kafka_conf_set_log_cb rd_kafka_conf_set_oauthbearer_token_refresh_cb
      rd_kafka_conf_set_opaque rd_kafka_conf_set_rebalance_cb rd_kafka_conf_set_stats_cb
      rd_kafka_consume_batch_queue rd_kafka_consume_batch_queue_nb rd_kafka_consumer_close
      rd_kafka_consumer_group_metadata rd_kafka_consumer_group_metadata_destroy
      rd_kafka_consumer_group_state_name rd_kafka_consumer_group_type_name rd_kafka_consumer_poll
      rd_kafka_consumer_poll_nb rd_kafka_destroy rd_kafka_err2name rd_kafka_err2str
      rd_kafka_error_code rd_kafka_error_destroy rd_kafka_error_is_fatal rd_kafka_error_is_retriable
      rd_kafka_error_string rd_kafka_error_txn_requires_abort
      rd_kafka_event_AlterConsumerGroupOffsets_result rd_kafka_event_CreateAcls_result
      rd_kafka_event_CreatePartitions_result rd_kafka_event_CreateTopics_result
      rd_kafka_event_DeleteAcls_result rd_kafka_event_DeleteConsumerGroupOffsets_result
      rd_kafka_event_DeleteGroups_result rd_kafka_event_DeleteRecords_result
      rd_kafka_event_DeleteTopics_result rd_kafka_event_DescribeAcls_result
      rd_kafka_event_DescribeCluster_result rd_kafka_event_DescribeConfigs_result
      rd_kafka_event_DescribeConsumerGroups_result rd_kafka_event_DescribeTopics_result
      rd_kafka_event_IncrementalAlterConfigs_result rd_kafka_event_ListConsumerGroups_result
      rd_kafka_event_ListOffsets_result rd_kafka_event_destroy rd_kafka_event_error
      rd_kafka_event_error_string rd_kafka_event_opaque rd_kafka_event_type rd_kafka_flush
      rd_kafka_get_err_descs rd_kafka_global_init rd_kafka_group_result_error
      rd_kafka_group_result_name rd_kafka_group_result_partitions rd_kafka_header_get_all
      rd_kafka_incremental_assign rd_kafka_incremental_unassign rd_kafka_mem_free rd_kafka_memberid
      rd_kafka_message_broker_id rd_kafka_message_destroy rd_kafka_message_headers
      rd_kafka_message_latency rd_kafka_message_status rd_kafka_message_timestamp rd_kafka_metadata
      rd_kafka_metadata_destroy rd_kafka_msg_partitioner_consistent
      rd_kafka_msg_partitioner_consistent_random rd_kafka_msg_partitioner_fnv1a
      rd_kafka_msg_partitioner_fnv1a_random rd_kafka_msg_partitioner_murmur2
      rd_kafka_msg_partitioner_murmur2_random rd_kafka_msg_partitioner_random rd_kafka_name
      rd_kafka_new rd_kafka_oauthbearer_set_token rd_kafka_oauthbearer_set_token_failure
      rd_kafka_offsets_for_times rd_kafka_offsets_store rd_kafka_outq_len rd_kafka_pause_partitions
      rd_kafka_poll rd_kafka_poll_nb rd_kafka_poll_set_consumer rd_kafka_position rd_kafka_producev
      rd_kafka_purge rd_kafka_query_watermark_offsets rd_kafka_queue_destroy
      rd_kafka_queue_get_background rd_kafka_queue_get_consumer rd_kafka_queue_get_main
      rd_kafka_queue_io_event_enable rd_kafka_rebalance_protocol rd_kafka_resume_partitions
      rd_kafka_seek rd_kafka_set_log_queue rd_kafka_subscribe rd_kafka_subscription
      rd_kafka_topic_conf_destroy rd_kafka_topic_conf_new rd_kafka_topic_conf_set
      rd_kafka_topic_destroy rd_kafka_topic_name rd_kafka_topic_new
      rd_kafka_topic_partition_get_leader_epoch rd_kafka_topic_partition_list_add
      rd_kafka_topic_partition_list_copy rd_kafka_topic_partition_list_destroy
      rd_kafka_topic_partition_list_new rd_kafka_topic_partition_list_set_offset
      rd_kafka_topic_result_error rd_kafka_topic_result_error_string rd_kafka_topic_result_name
      rd_kafka_unsubscribe typedef
    ]
  end

  let(:expected_constants) do
    %i[
      ConfigResource EMPTY_HASH ErrorCallback LogCallback Message NativeError NativeErrorDesc
      OAuthbearerTokenRefreshCallback PARTITIONERS RD_KAFKA_ACL_OPERATION_ALL
      RD_KAFKA_ACL_OPERATION_ALTER RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS RD_KAFKA_ACL_OPERATION_ANY
      RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION RD_KAFKA_ACL_OPERATION_CREATE
      RD_KAFKA_ACL_OPERATION_DELETE RD_KAFKA_ACL_OPERATION_DESCRIBE
      RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE
      RD_KAFKA_ACL_OPERATION_READ RD_KAFKA_ACL_OPERATION_WRITE RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW
      RD_KAFKA_ACL_PERMISSION_TYPE_ANY RD_KAFKA_ACL_PERMISSION_TYPE_DENY
      RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS RD_KAFKA_ADMIN_OP_CREATEACLS
      RD_KAFKA_ADMIN_OP_CREATEPARTITIONS RD_KAFKA_ADMIN_OP_CREATEPARTITIONS_RESULT
      RD_KAFKA_ADMIN_OP_CREATETOPICS RD_KAFKA_ADMIN_OP_DELETEACLS
      RD_KAFKA_ADMIN_OP_DELETECONSUMERGROUPOFFSETS RD_KAFKA_ADMIN_OP_DELETEGROUPS
      RD_KAFKA_ADMIN_OP_DELETERECORDS RD_KAFKA_ADMIN_OP_DELETETOPICS RD_KAFKA_ADMIN_OP_DESCRIBEACLS
      RD_KAFKA_ADMIN_OP_DESCRIBECLUSTER RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS
      RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS RD_KAFKA_ADMIN_OP_DESCRIBETOPICS
      RD_KAFKA_ADMIN_OP_INCREMENTALALTERCONFIGS RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS
      RD_KAFKA_ADMIN_OP_LISTOFFSETS RD_KAFKA_ALTER_CONFIG_OP_TYPE_APPEND
      RD_KAFKA_ALTER_CONFIG_OP_TYPE_DELETE RD_KAFKA_ALTER_CONFIG_OP_TYPE_SET
      RD_KAFKA_ALTER_CONFIG_OP_TYPE_SUBTRACT RD_KAFKA_CONSUMER_GROUP_STATE_COMPLETING_REBALANCE
      RD_KAFKA_CONSUMER_GROUP_STATE_DEAD RD_KAFKA_CONSUMER_GROUP_STATE_EMPTY
      RD_KAFKA_CONSUMER_GROUP_STATE_PREPARING_REBALANCE RD_KAFKA_CONSUMER_GROUP_STATE_STABLE
      RD_KAFKA_CONSUMER_GROUP_STATE_UNKNOWN RD_KAFKA_CONSUMER_GROUP_TYPE_CLASSIC
      RD_KAFKA_CONSUMER_GROUP_TYPE_CONSUMER RD_KAFKA_CONSUMER_GROUP_TYPE_UNKNOWN
      RD_KAFKA_EVENT_ALTERCONSUMERGROUPOFFSETS_RESULT RD_KAFKA_EVENT_CREATEACLS_RESULT
      RD_KAFKA_EVENT_CREATETOPICS_RESULT RD_KAFKA_EVENT_DELETEACLS_RESULT
      RD_KAFKA_EVENT_DELETECONSUMERGROUPOFFSETS_RESULT RD_KAFKA_EVENT_DELETEGROUPS_RESULT
      RD_KAFKA_EVENT_DELETERECORDS_RESULT RD_KAFKA_EVENT_DELETETOPICS_RESULT
      RD_KAFKA_EVENT_DESCRIBEACLS_RESULT RD_KAFKA_EVENT_DESCRIBECLUSTER_RESULT
      RD_KAFKA_EVENT_DESCRIBECONFIGS_RESULT RD_KAFKA_EVENT_DESCRIBECONSUMERGROUPS_RESULT
      RD_KAFKA_EVENT_DESCRIBETOPICS_RESULT RD_KAFKA_EVENT_INCREMENTALALTERCONFIGS_RESULT
      RD_KAFKA_EVENT_LISTCONSUMERGROUPS_RESULT RD_KAFKA_EVENT_LISTOFFSETS_RESULT
      RD_KAFKA_ISOLATION_LEVEL_READ_COMMITTED RD_KAFKA_ISOLATION_LEVEL_READ_UNCOMMITTED
      RD_KAFKA_MSG_F_COPY RD_KAFKA_MSG_STATUS_NOT_PERSISTED RD_KAFKA_MSG_STATUS_PERSISTED
      RD_KAFKA_MSG_STATUS_POSSIBLY_PERSISTED RD_KAFKA_OFFSET_BEGINNING RD_KAFKA_OFFSET_END
      RD_KAFKA_OFFSET_INVALID RD_KAFKA_OFFSET_SPEC_EARLIEST RD_KAFKA_OFFSET_SPEC_LATEST
      RD_KAFKA_OFFSET_SPEC_MAX_TIMESTAMP RD_KAFKA_OFFSET_STORED RD_KAFKA_PARTITION_UA
      RD_KAFKA_PARTITION_UA_STR RD_KAFKA_PURGE_F_INFLIGHT RD_KAFKA_PURGE_F_QUEUE
      RD_KAFKA_RESOURCE_ANY RD_KAFKA_RESOURCE_BROKER RD_KAFKA_RESOURCE_GROUP
      RD_KAFKA_RESOURCE_PATTERN_ANY RD_KAFKA_RESOURCE_PATTERN_LITERAL
      RD_KAFKA_RESOURCE_PATTERN_MATCH RD_KAFKA_RESOURCE_PATTERN_PREFIXED
      RD_KAFKA_RESOURCE_PATTERN_UNKNOWN RD_KAFKA_RESOURCE_TOPIC RD_KAFKA_RESOURCE_TRANSACTIONAL_ID
      RD_KAFKA_RESP_ERR_NO_ERROR RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS RD_KAFKA_RESP_ERR__NOENT
      RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS RD_KAFKA_RESP_ERR__STATE RD_KAFKA_RESP_ERR__TIMED_OUT
      RD_KAFKA_VTYPE_END RD_KAFKA_VTYPE_HEADER RD_KAFKA_VTYPE_HEADERS RD_KAFKA_VTYPE_KEY
      RD_KAFKA_VTYPE_MSGFLAGS RD_KAFKA_VTYPE_OPAQUE RD_KAFKA_VTYPE_PARTITION RD_KAFKA_VTYPE_RKT
      RD_KAFKA_VTYPE_TIMESTAMP RD_KAFKA_VTYPE_TOPIC RD_KAFKA_VTYPE_VALUE RebalanceCallback SizePtr
      StatsCallback TopicPartition TopicPartitionList
    ]
  end

  it "defines every expected method" do
    expect(described_class.singleton_methods).to include(*expected_methods)
  end

  it "defines every expected constant as its own constant" do
    expect(described_class.constants(false)).to include(*expected_constants)
  end

  it "keeps the callbacks as own constants so they can be replaced with remove_const/const_set" do
    %i[LogCallback StatsCallback ErrorCallback OAuthbearerTokenRefreshCallback RebalanceCallback].each do |name|
      expect(described_class.const_defined?(name, false)).to be(true)
      expect(described_class.const_get(name)).to be_a(FFI::Function)
    end
  end

  it "loads every group file from the bindings directory" do
    group_files = Dir[File.expand_path("../../../lib/rdkafka/bindings/*.rb", __dir__)]

    expect(group_files).not_to be_empty
    expect($LOADED_FEATURES).to include(*group_files)
  end
end
