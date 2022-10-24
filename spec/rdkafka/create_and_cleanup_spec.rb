# frozen_string_literal: true

require "spec_helper"

# See if we get a segfault when creating, using and closing lots
# of clients in different threads.

describe "creating lots of producers and consumers" do
  it "should not segfault" do
    100.times do |i|
      producer = rdkafka_producer_config.producer
      consumer = rdkafka_consumer_config(
        :"group.id" => "create_destroy"
      ).consumer

      producer.produce(
        topic: "create_destroy",
        payload: "payload",
        key: "key"
      ).wait

      tpl = Rdkafka::Consumer::TopicPartitionList.new
      tpl.add_topic("create_destroy", 1)
      consumer.assign(tpl)
      10.times do
        consumer.poll(10)
      end

      producer.close
      consumer.close
    end
  end
end
