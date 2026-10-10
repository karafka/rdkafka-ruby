# frozen_string_literal: true

module Rdkafka
  module Bindings
    LogCallback = FFI::Function.new(
      :void, [:pointer, :int, :string, :string]
    ) do |_client_ptr, level, _level_string, line|
      severity = case level
      when 0, 1, 2
        Logger::FATAL
      when 3
        Logger::ERROR
      when 4
        Logger::WARN
      when 5, 6
        Logger::INFO
      when 7
        Logger::DEBUG
      else
        Logger::UNKNOWN
      end

      Rdkafka::Config.ensure_log_thread
      Rdkafka::Config.log_queue << [severity, "rdkafka: #{line}"]
    end

    StatsCallback = FFI::Function.new(
      :int, [:pointer, :string, :int, :pointer]
    ) do |_client_ptr, json, _json_len, _opaque|
      if Rdkafka::Config.statistics_callback
        stats = JSON.parse(json)

        # If user requested statistics callbacks, we can use the statistics data to get the
        # partitions count for each topic when this data is published. That way we do not have
        # to query this information when user is using `partition_key`. This takes around 0.02ms
        # every statistics interval period (most likely every 5 seconds) and saves us from making
        # any queries to the cluster for the partition count.
        #
        # One edge case is if user would set the `statistics.interval.ms` much higher than the
        # default current partition count refresh (30 seconds). This is taken care of as the lack
        # of reporting to the partitions cache will cause cache expire and blocking refresh.
        #
        # If user sets `topic.metadata.refresh.interval.ms` too high this is on the user.
        #
        # Since this cache is shared, having few consumers and/or producers in one process will
        # automatically improve the querying times even with low refresh times.
        (stats["topics"] || EMPTY_HASH).each do |topic_name, details|
          partitions_count = details["partitions"].keys.count { |k| !(k == RD_KAFKA_PARTITION_UA_STR) }

          next unless partitions_count.positive?

          Rdkafka::Producer.partitions_count_cache.set(topic_name, partitions_count)
        end

        Rdkafka::Config.statistics_callback.call(stats)
      end

      # Return 0 so librdkafka frees the json string
      RD_KAFKA_RESP_ERR_NO_ERROR
    end

    ErrorCallback = FFI::Function.new(
      :void, [:pointer, :int, :string, :pointer]
    ) do |client_ptr, err_code, reason, _opaque|
      if Rdkafka::Config.error_callback
        instance_name = client_ptr.null? ? nil : Rdkafka::Bindings.rd_kafka_name(client_ptr)
        error = Rdkafka::RdkafkaError.new(err_code, broker_message: reason, instance_name: instance_name)
        error.set_backtrace(caller)
        Rdkafka::Config.error_callback.call(error)
      end
    end

    # The OAuth callback is currently global and contextless. This means that the callback will be
    # called for all instances, and the callback must be able to determine to which instance it is
    # associated. The instance name will be provided in the callback, allowing the callback to
    # reference the correct instance.
    #
    # An example of how to use the instance name in the callback is given below.
    # The `refresh_token` is configured as the `oauthbearer_token_refresh_callback`.
    # `instances` is a map of client names to client instances, maintained by the user.
    #
    # ```
    #   def refresh_token(config, client_name)
    #     client = instances[client_name]
    #     client.oauthbearer_set_token(
    #       token: 'new-token-value',
    #       lifetime_ms: token-lifetime-ms,
    #       principal_name: 'principal-name'
    #     )
    #   end
    # ```
    OAuthbearerTokenRefreshCallback = FFI::Function.new(
      :void, [:pointer, :string, :pointer]
    ) do |client_ptr, config, _opaque|
      if Rdkafka::Config.oauthbearer_token_refresh_callback && !client_ptr.null?
        Rdkafka::Config.oauthbearer_token_refresh_callback.call(config, Rdkafka::Bindings.rd_kafka_name(client_ptr))
      end
    end
  end
end
