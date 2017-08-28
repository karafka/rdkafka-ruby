require "pry"
require "rspec"
require "rdkafka"

def rdkafka_config
  Rdkafka::Config.new("bootstrap.servers" => "localhost:9092")
end
