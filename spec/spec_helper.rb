require "pry"
require "rspec"
require "rdkafka"

def rdkafka_config
  debug = if ENV["DEBUG_PRODUCER"]
            "broker,topic,msg"
          elsif ENV["DEBUG_CONSUMER"]
            "cgrp,topic,fetch"
          else
            ""
          end
  Rdkafka::Config.new(
    :"bootstrap.servers" => "localhost:9092",
    :"group.id" => "ruby_test",
    :"enable.partition.eof" => false,
    :"debug" => debug
  )
end
