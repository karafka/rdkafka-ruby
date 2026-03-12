# frozen_string_literal: true

# Base test class with per-test state reset
class Minitest::Test
  def setup
    ensure_topics_created
    Rdkafka::Config.statistics_callback = nil
    Rdkafka::Producer.partitions_count_cache.to_h.clear
  end
end
