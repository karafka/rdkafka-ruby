require "pry"
require "rspec"
require "rdkafka"

def rdkafka_config
  config = {
    :"bootstrap.servers" => "localhost:9092",
    :"group.id" => "ruby_test",
    :"client.id" => "test",
    :"auto.offset.reset" => "earliest",
    :"enable.partition.eof" => false
  }
  if ENV["DEBUG_PRODUCER"]
    config[:debug] = "broker,topic,msg"
  elsif ENV["DEBUG_CONSUMER"]
    config[:debug] = "cgrp,topic,fetch"
  end
  Rdkafka::Config.new(config)
end

def wait_for_message(topic:, delivery_report:, timeout_in_seconds: 30)
  offset = delivery_report.offset - 1
  consumer = rdkafka_config.consumer
  consumer.subscribe(topic)
  timeout = Time.now.to_i + timeout_in_seconds
  loop do
    if timeout <= Time.now.to_i
      raise "Timeout of #{timeout_in_seconds} seconds reached in wait_for_message"
    end
    message = consumer.poll(100)
    if message && message.offset == offset
      return message
    end
  end
ensure
  consumer.close
end
