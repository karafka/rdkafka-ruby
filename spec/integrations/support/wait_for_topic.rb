# frozen_string_literal: true

require "rdkafka"

# Waits for a just-created topic to show up in metadata.
#
# A create topic ack does not mean the topic is visible yet: for a moment the broker can still
# answer with an error such as `unknown_topic_or_part`, which `#metadata` raises, or report fewer
# partitions than were requested. Producing to or reading the topic before then makes a spec flaky.
module WaitForTopic
  class << self
    # Polls metadata until the topic is visible with its partitions, or raises once the timeout
    # passes
    #
    # @param client [Rdkafka::Admin, Rdkafka::Producer, Rdkafka::Consumer] client to query
    #   metadata with
    # @param topic [String] topic name
    # @param partitions [Integer, nil] partition count to wait for; when nil, any partition will do
    # @param timeout [Numeric] seconds to wait before giving up
    # @raise [RuntimeError] when the topic is not visible in time, with the last error as its cause
    def wait_for_topic(client, topic, partitions: nil, timeout: 30)
      deadline = now + timeout
      last_error = nil

      loop do
        begin
          count = client.metadata(topic).topics.first&.fetch(:partition_count, 0).to_i
          return if count.positive? && (partitions.nil? || count == partitions)
        rescue Rdkafka::RdkafkaError => e
          last_error = e
        end

        if now > deadline
          raise(
            "Topic #{topic} not visible in metadata after #{timeout}s " \
              "(last error: #{last_error&.code.inspect})",
            cause: last_error
          )
        end

        sleep(0.1)
      end
    end

    private

    # @return [Float] monotonic clock in seconds
    def now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end
end
