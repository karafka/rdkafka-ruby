# frozen_string_literal: true

require "rdkafka"

# librdkafka's in-process mock cluster, for integration specs that need broker behaviour a real
# broker does not produce on request, such as injected request errors. Needs no Kafka broker.
#
# Run each scenario on its own cluster: injected errors that a scenario never triggers stay
# queued and the next request of that API on the same cluster gets them.
#
# @example
#   MockCluster.run do |cluster|
#     cluster.create_topic("events", 1)
#     cluster.push_request_errors(:offset_fetch, :not_coordinator)
#     consumer = cluster.config("group.id": "group").consumer
#   end
class MockCluster
  # Kafka protocol API keys of the requests that errors can be injected into
  API_KEYS = {
    offset_commit: 8,
    offset_fetch: 9
  }.freeze

  # Kafka protocol error codes, named as in `Rdkafka::RdkafkaError#code`
  ERROR_CODES = {
    coordinator_not_available: 15,
    not_coordinator: 16
  }.freeze

  # librdkafka mock cluster API, not part of the public bindings
  module Bindings
    extend FFI::Library

    ffi_lib Rdkafka::Bindings.ffi_libraries.map(&:name)

    attach_function :rd_kafka_mock_cluster_new, %i[pointer int], :pointer
    attach_function :rd_kafka_mock_cluster_destroy, %i[pointer], :void
    attach_function :rd_kafka_mock_cluster_bootstraps, %i[pointer], :string
    attach_function :rd_kafka_mock_topic_create, %i[pointer string int int], :int
    attach_function :rd_kafka_mock_push_request_errors_array, %i[pointer int16 size_t pointer], :void
  end

  # @return [String] bootstrap servers of the cluster
  attr_reader :bootstrap_servers

  class << self
    # Yields a new cluster and destroys it afterwards. Close the clients that use the cluster
    # inside the block.
    #
    # @param broker_count [Integer] number of mock brokers
    # @yieldparam cluster [MockCluster] the new cluster
    def run(broker_count: 1)
      cluster = new(broker_count: broker_count)
      yield(cluster)
    ensure
      cluster&.destroy
    end
  end

  # @param broker_count [Integer] number of mock brokers
  def initialize(broker_count: 1)
    # librdkafka needs a client to host the mock cluster
    @host = Rdkafka::Config.new.producer
    @cluster = @host.instance_variable_get(:@native_kafka).with_inner do |inner|
      Bindings.rd_kafka_mock_cluster_new(inner, broker_count)
    end
    @bootstrap_servers = Bindings.rd_kafka_mock_cluster_bootstraps(@cluster)
  end

  # @param topic_name [String] name of the topic to create
  # @param partition_count [Integer] number of partitions
  # @param replication_factor [Integer] number of replicas
  # @raise [Rdkafka::RdkafkaError] when the topic cannot be created
  def create_topic(topic_name, partition_count, replication_factor = 1)
    response = Bindings.rd_kafka_mock_topic_create(
      @cluster, topic_name, partition_count, replication_factor
    )

    raise Rdkafka::RdkafkaError.new(response) unless response.zero?
  end

  # Fails the next requests of the given API with the given errors, one error per request, in
  # the given order
  #
  # @param api [Symbol] request API, a key of `API_KEYS`
  # @param errors [Array<Symbol>] errors to return, keys of `ERROR_CODES`
  # @raise [KeyError] when the API or an error name is unknown
  def push_request_errors(api, *errors)
    codes = errors.map { |error| ERROR_CODES.fetch(error) }
    codes_ptr = FFI::MemoryPointer.new(:int, codes.size)
    codes_ptr.write_array_of_int(codes)

    Bindings.rd_kafka_mock_push_request_errors_array(
      @cluster, API_KEYS.fetch(api), codes.size, codes_ptr
    )
  end

  # @param settings [Hash] extra client settings
  # @return [Rdkafka::Config] client config pointing at this cluster
  def config(settings = {})
    Rdkafka::Config.new({ "bootstrap.servers": bootstrap_servers }.merge(settings))
  end

  # Destroys the cluster and closes its host client. librdkafka requires the cluster to be
  # destroyed before its host client.
  #
  # @return [void]
  def destroy
    Bindings.rd_kafka_mock_cluster_destroy(@cluster)
    @host.close
  end
end
