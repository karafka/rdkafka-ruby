#!/usr/bin/env ruby

# Add lib path to LOAD_PATH
$LOAD_PATH.unshift File.expand_path('../../../lib', __FILE__)

require 'optparse'
require 'ostruct'
require 'rdkafka'

class RebalanceListener
  attr_accessor :params

  def initialize(params)
    @params = params
  end

  def on_partitions_assigned(consumer, list)
    start_offset = params[:"start-offset"]
    unless start_offset.nil?
      list.each do |topic, partitions|
        partitions.each do |partition|
          puts "Assigning partition #{partition.partition} of #{topic} to #{start_offset}"
          partition.offset = start_offset
        end
      end
    end
    consumer.assign(list)
  end

  def on_partitions_revoked(consumer, list)
    consumer.assign(nil)
  end
end

params = {}
optparse = OptionParser.new do |opts|
  opts.banner = "Usage: seek_rebalance_consumer.rb [options]"
  opts.on("-bSERVER", "--bootstrap-server=SERVER", "Bootstrap server. Defaults to localhost:9092")
  opts.on("-gID", "--group=ID", "[Required] Consumer group ID")
  opts.on("-tTOPIC_NAME", "--topic=TOPIC_NAME", "[Required] Name of Topic to subscribe to")
  opts.on("-sOFFSET", "--start-offset=OFFSET",
    "Start Offset. If unset, will not seek on start. If -1, will seek to beginning.", Integer)
  opts.on("-eOFFSET", "--end-offset=OFFSET",
    "End Offset. If set it will seek to 0 on reaching this offset", Integer)
  opts.on("-wSECONDS", "--wait=SECONDS",
    "Number of seconds to wait between each message. Defaults to 1", Float)
  opts.on("-h", "--help") do
    puts opts
    exit
  end
end
optparse.parse!(into: params)

params[:"bootstrap-server"] ||= "localhost:9092"
params[:"wait"] ||= 1
if !params.has_key?(:group) || !params.has_key?(:topic)
  puts optparse.help
  exit 1
end

cfg = Rdkafka::Config.new(
  "bootstrap.servers": params[:"bootstrap-server"],
  "group.id": params[:group],
  "auto.offset.reset": "earliest",
)
cfg.consumer_rebalance_listener = RebalanceListener.new(params)
c = cfg.consumer

trap("QUIT") { c.close }
trap("INT") { c.close }
trap("TERM") { c.close }

c.subscribe(params[:topic])
puts "Subscribed to #{params[:topic]}"

end_offset = params[:"end-offset"]
c.each do |message|
  puts "Got #{message.topic}/#{message.partition}@#{message.offset} #{message.payload}"
  if !end_offset.nil? && message.offset >= end_offset
    puts "Seeking back to 0 for #{message.topic}, #{message.partition}"
    c.seek(OpenStruct.new(topic: message.topic, partition: message.partition, offset: 0))

    puts "Commiting the updated offsets immediately"
    tpl = Rdkafka::Consumer::TopicPartitionList.new
    tpl.add_topic_and_partitions_with_offsets(message.topic, {message.partition => 0})
    c.commit(tpl)
  end

  # Sleep 1 second to slow down the consumption so it is easier to follow.
  # Not needed in the real code.
  sleep params[:"wait"]
end

puts "Done"
