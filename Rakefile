# frozen_string_literal: true

# Rakefile

require 'bundler/gem_tasks'
require "./lib/rdkafka"

desc 'Generate some message traffic'
task :produce_messages do
  config = {:"bootstrap.servers" => "localhost:9092"}
  if ENV["DEBUG"]
    config[:debug] = "broker,topic,msg"
  end
  producer = Rdkafka::Config.new(config).producer

  delivery_handles = []
  100.times do |i|
    puts "Producing message #{i}"
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

desc 'Consume some messages'
task :consume_messages do
  config = {
    :"bootstrap.servers" => "localhost:9092",
    :"group.id" => "rake_test",
    :"enable.partition.eof" => false,
    :"auto.offset.reset" => "earliest",
    :"statistics.interval.ms" => 10_000
  }
  if ENV["DEBUG"]
    config[:debug] = "cgrp,topic,fetch"
  end
  Rdkafka::Config.statistics_callback = lambda do |stats|
    puts stats
  end
  consumer = Rdkafka::Config.new(config).consumer
  consumer = Rdkafka::Config.new(config).consumer
  consumer.subscribe("rake_test_topic")
  consumer.each do |message|
    puts "Message received: #{message}"
  end
end

desc 'Hammer down'
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
