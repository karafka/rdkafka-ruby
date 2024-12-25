require 'rdkafka'

config = {
  :"bootstrap.servers" => "localhost:9092",
  :"group.id" => "ruby-test3",
  :'auto.offset.reset' => 'earliest',
}
producer = Rdkafka::Config.new(config).producer
delivery_handles = []

100.times do |i|
  puts "Producing message #{i}"
  delivery_handles << producer.produce(
      topic:   "ruby-test-topic",
      payload: "Payload #{i}",
      key:     "Key #{i}"
  )
end


delivery_handles.each(&:wait)

consumer = Rdkafka::Config.new(config).consumer
consumer.subscribe("ruby-test-topic")

consumer.each do |message|
  puts "Message received: #{message}"
  consumer.store_offset(message)
end
