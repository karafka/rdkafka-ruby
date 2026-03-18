# frozen_string_literal: true

# Module to hold dynamically generated test topics with UUIDs.
# Topics are generated once when first accessed and then cached.
module TestTopics
  extend KafkaConfigHelpers
  extend KafkaWaitHelpers

  class << self
    # Generates a unique topic name with an +it-+ prefix and a random UUID.
    #
    # @return [String] unique topic name
    def unique
      "it-#{SecureRandom.uuid}"
    end

    # Returns a cached example topic name, generating it once on first access.
    #
    # @return [String] the example topic name
    def example_topic
      @example_topic ||= unique
    end

    # Creates a new Kafka topic with a unique name and waits until it is available.
    #
    # @param partitions [Integer] the number of partitions for the topic
    # @return [String] the created topic name
    def create(partitions: 3)
      topic_name = unique
      admin = rdkafka_config.admin
      begin
        handle = admin.create_topic(topic_name, partitions, 1)
        handle.wait(max_wait_timeout_ms: 15_000)
        wait_for_topic(admin, topic_name)
        topic_name
      ensure
        admin.close
      end
    end
  end
end
