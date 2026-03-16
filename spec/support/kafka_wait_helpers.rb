# frozen_string_literal: true

# Polling and wait helper methods for Kafka tests.
# Can be included as instance methods or called as module methods.
module KafkaWaitHelpers
  extend self

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

  def wait_for_assignment(consumer)
    10.times do
      break if !consumer.assignment.empty?
      sleep 1
    end
  end

  def wait_for_unassignment(consumer)
    10.times do
      break if consumer.assignment.empty?
      sleep 1
    end
  end

  def wait_for_topic(admin, topic)
    admin.metadata(topic)
  rescue Rdkafka::RdkafkaError => e
    raise unless e.code == :unknown_topic_or_part

    sleep(0.5)

    retry
  end

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
