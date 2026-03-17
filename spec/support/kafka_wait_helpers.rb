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
    consumer ||= rdkafka_consumer_config("allow.auto.create.topics": true).consumer

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
