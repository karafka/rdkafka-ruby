require_relative "../lib/rdkafka"

$kafka_config = {
  :"bootstrap.servers" => "localhost:9092",
  :"group.id" => "rake_test",
  :"enable.partition.eof" => false,
  :"auto.offset.reset" => "earliest",
  :"statistics.interval.ms" => 10_000
}

if ENV["DEBUG"]
  $kafka_config[:debug] = "cgrp,topic,fetch"

  Rdkafka::Config.statistics_callback = lambda do |stats|
    puts stats
  end
end


def prepare
  producer = Rdkafka::Config.new($kafka_config).producer
  messages_num = ENV['MESSAGES_NUM'] ? ENV['MESSAGES_NUM'].to_i : 100_000
  delivery_handles = []
  messages_num.times do |i|
    puts "Producing message #{i}" if i % 5000 == 0
    delivery_handles << producer.produce(
        topic:   "rake_test_topic",
        payload: "Payload #{i} from Rake",
        key:     "Key #{i} from Rake"
    )
  end
  puts 'Waiting for delivery'
  delivery_handles.each(&:wait)
  puts 'Done'
end

def consume_with_queue
  consumer = Rdkafka::Config.new($kafka_config).consumer

  stats = {}
  Thread.new do
    loop do
      p [:stats, stats]
      sleep 1
    end
  end

  consumer.consume_with_queue("rake_test_topic") do |message|
    #p message
    stats[Time.now.to_i] ||= 0
    stats[Time.now.to_i] += 1
  end
end

def consume_with_poll
  consumer = Rdkafka::Config.new($kafka_config).consumer

  stats = {}
  Thread.new do
    loop do
      p [:stats, stats]
      sleep 1
    end
  end

  consumer.subscribe("rake_test_topic")
  consumer.each do |message|
    #p message
    stats[Time.now.to_i] ||= 0
    stats[Time.now.to_i] += 1
  end
end

def reset
  consumer = Rdkafka::Config.new($kafka_config).consumer
  consumer.reset_offsets("rake_test_topic")

  #system("kt group -group rake_test -topic rake_test_topic")
end

# HOW TO USE:
# 1. run prepare()
# 2. run reset() and consume_with_poll() or consume_with_queue() to compare

#prepare
reset
consume_with_poll
#consume_with_queue
