# frozen_string_literal: true

module Rdkafka
  module Helpers
    # Shared `#metadata` implementation for Admin, Consumer and Producer.
    #
    # `rd_kafka_metadata()` is handle-agnostic in librdkafka - it works on any `rd_kafka_t`
    # (consumer, producer or admin) - so this Ruby-level implementation is identical across all
    # three client types. Includers must provide a private `#closed_check(method)` that raises
    # their own `Closed*Error`.
    module Metadata
      # Performs the metadata request using this client
      #
      # @param topic_name [String, nil] metadata about particular topic or all if nil
      # @param timeout_ms [Integer] metadata request timeout
      # @return [Rdkafka::Metadata] requested metadata
      def metadata(topic_name = nil, timeout_ms = Defaults::METADATA_TIMEOUT_MS)
        closed_check(__method__)

        @native_kafka.with_inner do |inner|
          # Must stay fully qualified: this module is itself named `Metadata`, so a bare
          # `Metadata.new` here would resolve to `Rdkafka::Helpers::Metadata` (no `.new`) instead
          # of this class.
          Rdkafka::Metadata.new(inner, topic_name, timeout_ms)
        end
      end
    end
  end
end
