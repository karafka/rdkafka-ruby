# frozen_string_literal: true

require "spec_helper"

# See if we get a segfault when creating, using and closing lots
# of clients in different threads.

describe "creating lots of producers and consumers in threads" do
  it "should not segfault" do
    threads = []

    100.times do |i|
      threads << Thread.new do
        producer = rdkafka_producer_config.producer
        consumer = rdkafka_consumer_config.consumer

        producer.produce(
          topic: "load_test_topic",
          payload: "payload",
          key: "key"
        ).wait

        consumer.subscribe("load_test_topic")
        consumer.poll

        producer.close
        consumer.close
        admin.close
      end
    end

    threads.each(&:join)
  end
end
