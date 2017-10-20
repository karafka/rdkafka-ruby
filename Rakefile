require "./lib/rdkafka"

task :create_topics do
  puts "Creating test topics"
  kafka_topics = if ENV['TRAVIS']
                   'kafka/bin/kafka-topics.sh'
                 else
                   'kafka-topics'
                 end
  `#{kafka_topics} --create --topic=consume_test_topic --zookeeper=127.0.0.1:2181 --partitions=3 --replication-factor=1`
  `#{kafka_topics} --create --topic=empty_test_topic --zookeeper=127.0.0.1:2181 --partitions=3 --replication-factor=1`
  `#{kafka_topics} --create --topic=load_test_topic --zookeeper=127.0.0.1:2181 --partitions=3 --replication-factor=1`
  `#{kafka_topics} --create --topic=produce_test_topic --zookeeper=127.0.0.1:2181 --partitions=3 --replication-factor=1`
  `#{kafka_topics} --create --topic=rake_test_topic --zookeeper=127.0.0.1:2181 --partitions=3 --replication-factor=1`
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
    puts "Message received: #{message}"
  end
end

task :load_test do
  puts "Starting load test"

  config = Rdkafka::Config.new(
    :"bootstrap.servers" => "localhost:9092",
    :"group.id" => "load-test",
    :"enable.partition.eof" => false
  )

  # Create a producer in a thread
  Thread.new do
    producer = config.producer
    loop do
      handles = []
      1000.times do |i|
        handles.push(producer.produce(
          topic:   "load_test_topic",
          payload: "Payload #{i}",
          key:     "Key #{i}"
        ))
      end
      handles.each(&:wait)
      puts "Produced 1000 messages"
    end
  end.abort_on_exception = true

  # Create three consumers in threads
  3.times do |i|
    Thread.new do
      count = 0
      consumer = config.consumer
      consumer.subscribe("load_test_topic")
      consumer.each do |message|
        count += 1
        if count % 1000 == 0
          puts "Received 1000 messages in thread #{i}"
        end
      end
    end.abort_on_exception = true
  end

  loop do
    sleep 1
  end
end
