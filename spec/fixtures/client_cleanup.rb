# frozen_string_literal: true

at_exit do
  result = Rdkafka::Bindings.rd_kafka_wait_destroyed(5_000)
  raise "native clients are still alive" unless result.zero?
end

load File.expand_path("../../lib/rdkafka.rb", __dir__)

Rdkafka::Bindings.attach_function :rd_kafka_wait_destroyed, [:int], :int, blocking: true

config = { "bootstrap.servers" => "127.0.0.1:1" }
$clients = [
  Rdkafka::Config.new(config.merge("group.id" => "client-cleanup-spec")).consumer,
  Rdkafka::Config.new(config).producer,
  Rdkafka::Config.new(config).admin
]
