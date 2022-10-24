# frozen_string_literal: true

require "spec_helper"

# See if we get a segfault when creating, using and closing lots
# of clients in different threads.

describe "creating lots of producers and consumers" do
  it "should not segfault" do
    25.times do |i|
      producer = rdkafka_producer_config.producer
      consumer = rdkafka_consumer_config(
        :"group.id" => "load_test"
      ).consumer

      producer.produce(
        topic: "load_test_topic",
        payload: "payload",
        key: "key"
      ).wait

      consumer.subscribe("load_test_topic")
      consumer.poll(100)

      producer.close
      consumer.close
    end
  end
end
