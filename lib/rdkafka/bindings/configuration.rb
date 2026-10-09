# frozen_string_literal: true

module Rdkafka
  module Bindings
    # Configuration

    enum :kafka_config_response, [
      :config_unknown, -2,
      :config_invalid, -1,
      :config_ok, 0
    ]

    attach_function :rd_kafka_conf_new, [], :pointer
    attach_function :rd_kafka_conf_set, [:pointer, :string, :string, :pointer, :int], :kafka_config_response
    attach_function :rd_kafka_conf_get, [:pointer, :string, :pointer, :pointer], :kafka_config_response
    attach_function :rd_kafka_conf, [:pointer], :pointer
    attach_function :rd_kafka_conf_dump, [:pointer, :pointer], :pointer
    attach_function :rd_kafka_conf_dump_free, [:pointer, :size_t], :void
    attach_function :rd_kafka_conf_destroy, [:pointer], :void
    callback :log_cb, [:pointer, :int, :string, :string], :void
    attach_function :rd_kafka_conf_set_log_cb, [:pointer, :log_cb], :void
    attach_function :rd_kafka_conf_set_opaque, [:pointer, :pointer], :void
    callback :stats_cb, [:pointer, :string, :int, :pointer], :int
    attach_function :rd_kafka_conf_set_stats_cb, [:pointer, :stats_cb], :void
    callback :error_cb, [:pointer, :int, :string, :pointer], :void
    attach_function :rd_kafka_conf_set_error_cb, [:pointer, :error_cb], :void
    attach_function :rd_kafka_rebalance_protocol, [:pointer], :string
    callback :oauthbearer_token_refresh_cb, [:pointer, :string, :pointer], :void
    attach_function :rd_kafka_conf_set_oauthbearer_token_refresh_cb, [:pointer, :oauthbearer_token_refresh_cb], :void
    attach_function :rd_kafka_oauthbearer_set_token, [:pointer, :string, :int64, :pointer, :pointer, :int, :pointer, :int], :int
    attach_function :rd_kafka_oauthbearer_set_token_failure, [:pointer, :string], :int
    # Log queue
    attach_function :rd_kafka_set_log_queue, [:pointer, :pointer], :void
    attach_function :rd_kafka_queue_get_main, [:pointer], :pointer
    attach_function :rd_kafka_queue_get_background, [:pointer], :pointer

    # Queue IO Event Support - for fiber scheduler integration
    # Enables notifications to a custom FD when queue transitions from empty to non-empty
    # Arguments:
    # - queue (rd_kafka_queue_t*) - the queue to monitor
    # - fd (int) - file descriptor to write to (provide your own pipe/eventfd)
    # - payload (const void*) - data to write to fd
    # - size (size_t) - size of payload
    attach_function :rd_kafka_queue_io_event_enable, [:pointer, :int, :pointer, :size_t], :void
    # Per topic configs
    attach_function :rd_kafka_topic_conf_new, [], :pointer
    attach_function :rd_kafka_topic_conf_destroy, [:pointer], :void
    attach_function :rd_kafka_topic_conf_set, [:pointer, :string, :string, :pointer, :int], :kafka_config_response
  end
end
