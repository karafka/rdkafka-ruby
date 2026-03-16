# frozen_string_literal: true

# Module to hold dynamically generated test topics with UUIDs.
# Topics are generated once when first accessed and then cached.
module TestTopics
  extend KafkaConfigHelpers
  extend KafkaWaitHelpers

  class << self
    # Generate a unique topic name with it- prefix and UUID
    def unique
      "it-#{SecureRandom.uuid}"
    end

    def example_topic
      @example_topic ||= unique
    end

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
