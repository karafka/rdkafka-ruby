# frozen_string_literal: true

# Module to hold dynamically generated test topics with UUIDs.
# Topics are generated once when first accessed and then cached.
module TestTopics
  extend KafkaConfigHelpers
  extend KafkaWaitHelpers

  SPEC_HASH = begin
    gem_root = File.expand_path(File.join(__dir__, "..", ".."))
    absolute_program = File.expand_path($PROGRAM_NAME)
    relative_path = absolute_program.sub("#{gem_root}/", "")
    Digest::MD5.hexdigest(relative_path)[0, 6]
  end

  private_constant :SPEC_HASH

  class << self
    # Returns the per-file spec hash for embedding in topic/group names.
    #
    # @return [String] 6-character hex hash
    def spec_hash
      SPEC_HASH
    end

    # Generates a unique topic name with an +it-+ prefix, spec hash, and a random UUID.
    #
    # @return [String] unique topic name
    def unique
      "it-#{SPEC_HASH}-#{SecureRandom.uuid}"
    end

    # Generates a unique topic name for tests that intentionally operate on non-existing topics.
    # Uses an +it-ne-+ prefix so Kafka auto-creation warnings for these topics can be
    # distinguished from unexpected warnings in the warning verification script.
    #
    # @return [String] unique non-existing topic name
    def non_existing
      "it-ne-#{SPEC_HASH}-#{SecureRandom.uuid}"
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
