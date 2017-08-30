require "pry"
require "rspec"
require "rdkafka"

def rdkafka_config
  config = {
    :"bootstrap.servers" => "localhost:9092",
    :"group.id" => "ruby_test",
    :"enable.partition.eof" => false
  }
  if ENV["DEBUG_PRODUCER"]
    config[:debug] = "broker,topic,msg"
  elsif ENV["DEBUG_CONSUMER"]
    config[:debug] = "cgrp,topic,fetch"
  end
  Rdkafka::Config.new(config)
end
