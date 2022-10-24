# frozen_string_literal: true

require "spec_helper"

# See if we get a segfault when creating, using and closing lots
# of clients in different threads.

describe "creating lots of producers and consumers in threads" do
  it "should not segfault" do
    threads = []

    config = Rdkafka::Config.new

    25.times do |i|
      threads << Thread.new do
        producer = config.producer
        consumer = config.consumer
        admin = config.admin

        topic = "load-topic-#{i}"

        admin.create_topic(topic, 1, 1).wait(max_wait_timeout: 2.0)

        producer.produce(
          topic: topic,
          payload: "payload",
          key: "key"
        ).wait

        consumer.subscribe(topic)
        consumer.next

        producer.close
        consumer.close
        admin.close
      end
    end

    threads.each(&:join)
  end
end
