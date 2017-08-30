require "./lib/rdkafka"

task :create_topics do
  `kafka-topics --create --topic=produce_test_topic --zookeeper=127.0.0.1:2181 --partitions=1 --replication-factor=1`
  `kafka-topics --create --topic=rake_test_topic --zookeeper=127.0.0.1:2181 --partitions=1 --replication-factor=1`
end

task :produce_messages do
  producer = Rdkafka::Config.new(
    :"bootstrap.servers" => "localhost:9092"
  ).producer
  100.times do |i|
    producer.produce(
        topic:   "rake_test_topic",
        payload: "Payload #{i} from Rake",
        key:     "Key #{i} from Rake"
    ).wait
  end
end

task :consume_messages do
  consumer = Rdkafka::Config.new(
    :"bootstrap.servers" => "localhost:9092",
    :"group.id" => "rake_test",
    :"enable.partition.eof" => false,
    :"auto.offset.reset" => "earliest"
  ).consumer
  consumer.subscribe("rake_test_topic")
  consumer.each do |message|
    puts message
  end
end
