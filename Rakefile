require "./lib/rdkafka"

task :create_topics do
  puts "Creating test topics"
  `kafka-topics --create --topic=produce_test_topic --zookeeper=127.0.0.1:2181 --partitions=3 --replication-factor=1`
  `kafka-topics --create --topic=rake_test_topic --zookeeper=127.0.0.1:2181 --partitions=3 --replication-factor=1`
end

task :produce_messages do
  config = {:"bootstrap.servers" => "localhost:9092"}
  if ENV["DEBUG"]
    config[:debug] = "broker,topic,msg"
  end
  producer = Rdkafka::Config.new(config).producer
  100.times do |i|
    puts "Producing message #{i}"
    producer.produce(
        topic:   "rake_test_topic",
        payload: "Payload #{i} from Rake",
        key:     "Key #{i} from Rake"
    ).wait
  end
end

task :consume_messages do
  config = {
    :"bootstrap.servers" => "localhost:9092",
    :"group.id" => "rake_test",
    :"enable.partition.eof" => false,
    :"auto.offset.reset" => "earliest"
  }
  if ENV["DEBUG"]
    config[:debug] = "cgrp,topic,fetch"
  end
  consumer = Rdkafka::Config.new(config).consumer
  consumer.subscribe("rake_test_topic")
  consumer.each do |message|
    puts message
  end
end
