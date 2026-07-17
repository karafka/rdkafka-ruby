# frozen_string_literal: true

# Polling and wait helper methods for Kafka tests.
# Can be included as instance methods or called as module methods.
module KafkaWaitHelpers
  extend self

  # Polls a Kafka topic until the message matching the given delivery report is found.
  # Creates a temporary consumer with direct partition assignment if none is provided.
  #
  # @param topic [String] the topic name to consume from
  # @param delivery_report [Rdkafka::Producer::DeliveryReport] report identifying the target message
  # @param timeout_in_seconds [Integer] maximum seconds to wait before raising
  # @param consumer [Rdkafka::Consumer, nil] optional existing consumer to reuse
  # @return [Rdkafka::Consumer::Message] the consumed message matching the delivery report
  # @raise [RuntimeError] if the timeout is reached without finding the message
  def wait_for_message(topic:, delivery_report:, timeout_in_seconds: 30, consumer: nil)
    new_consumer = consumer.nil?
    consumer ||= rdkafka_consumer_config.consumer

    if new_consumer
      # Use direct partition assignment to avoid group coordination delays
      tpl = Rdkafka::Consumer::TopicPartitionList.new
      tpl.add_topic_and_partitions_with_offsets(
        topic,
        delivery_report.partition => delivery_report.offset
      )
      consumer.assign(tpl)
    else
      consumer.subscribe(topic)
      wait_for_assignment(consumer)
    end

    timeout = Time.now.to_i + timeout_in_seconds

    loop do
      if timeout <= Time.now.to_i
        raise "Timeout of #{timeout_in_seconds} seconds reached in wait_for_message"
      end

      message = consumer.poll(100)
      if message &&
          message.partition == delivery_report.partition &&
          message.offset == delivery_report.offset
        return message
      end
    end
  ensure
    consumer.close if new_consumer
  end

  # Polls until the consumer has a non-empty partition assignment, up to 10 seconds.
  #
  # @param consumer [Rdkafka::Consumer] the consumer to check for assignment
  # @return [void]
  def wait_for_assignment(consumer)
    10.times do
      break if !consumer.assignment.empty?
      sleep 1
    end
  end

  # Polls until the consumer's partition assignment is empty, up to 10 seconds.
  #
  # @param consumer [Rdkafka::Consumer] the consumer to check for unassignment
  # @return [void]
  def wait_for_unassignment(consumer)
    10.times do
      break if consumer.assignment.empty?
      sleep 1
    end
  end

  # Polls the broker until the given topic is available in metadata.
  # Retries on unknown_topic_or_part errors with a short sleep.
  #
  # @param admin [Rdkafka::Admin] admin client to query metadata from
  # @param topic [String] the topic name to wait for
  # @return [void]
  def wait_for_topic(admin, topic)
    admin.metadata(topic)
  rescue Rdkafka::RdkafkaError => e
    raise unless e.code == :unknown_topic_or_part

    sleep(0.5)

    retry
  end

  # Produces (and waits for) a message to each given partition so their leaders are actually
  # serving before the test queries them.
  #
  # Right after a topic is created, a partition can appear in metadata with an elected leader
  # before that broker is ready to serve it, so a ListOffsets/fetch request still fails with
  # +not_leader_for_partition+. A metadata check is therefore not enough. A successful produce is a
  # full round-trip to the leader - librdkafka retries it internally until leadership settles - so
  # once it completes the partition is genuinely ready to be queried.
  #
  # @param topic [String] the topic to produce to
  # @param partitions [Array<Integer>] the partitions to warm up
  # @return [void]
  def warm_up_partitions(topic, *partitions)
    producer = rdkafka_config.producer

    partitions.each do |partition|
      producer.produce(topic: topic, payload: "warmup", partition: partition).wait
    end
  ensure
    producer&.close
  end

  # Polls +describe_configs+ for a resource until the named config reports the
  # expected value, or the timeout elapses. Broker acknowledgment of an
  # +incremental_alter_configs+ request does not guarantee the change is
  # immediately visible to a subsequent +describe_configs+, especially on slow
  # CI runners, so a fixed sleep is unreliable.
  #
  # @param admin [Rdkafka::Admin] admin client to query
  # @param resources [Array<Hash>] describe_configs resources argument
  # @param config_name [String] the config entry name to inspect
  # @param expected [String] the value to wait for
  # @param timeout_in_seconds [Integer] maximum seconds to wait before raising
  # @return [Object] the matching config binding result
  # @raise [RuntimeError] if the timeout is reached without the expected value
  def wait_for_config_value(admin, resources:, config_name:, expected:, timeout_in_seconds: 30)
    deadline = Time.now.to_f + timeout_in_seconds
    last_value = nil

    loop do
      config = admin.describe_configs(resources).wait.resources.first.configs.find do |c|
        c.name == config_name
      end
      last_value = config&.value
      return config if last_value == expected

      if Time.now.to_f >= deadline
        raise "Timeout waiting for #{config_name} to become #{expected.inspect} " \
          "(last value: #{last_value.inspect})"
      end

      sleep(0.5)
    end
  end

  # Subscribes to a topic, polls once, executes the given block, then unsubscribes
  # and closes the consumer. Used to trigger consumer group listener callbacks.
  #
  # @param listener [Object] the listener object (unused directly, provided for context)
  # @param topic [String] the topic name to subscribe to
  # @yield optional block to execute between subscribe and unsubscribe
  # @return [void]
  def notify_listener(listener, topic:, &block)
    # 1. subscribe and poll
    consumer.subscribe(topic)
    wait_for_assignment(consumer)
    consumer.poll(100)

    block&.call

    # 2. unsubscribe
    consumer.unsubscribe
    wait_for_unassignment(consumer)
    consumer.close
  end
end
