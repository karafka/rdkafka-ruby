module Rdkafka
  class MetadataBroker
    def self.from_native(p)
      n = Rdkafka::Bindings::MetadataBroker.new(p)
      MetadataBroker.new(
        id: n[:id],
        host: n[:host],
        port: n[:port]
      )
    end
    attr_reader :id, :host, :port

    def initialize(id:, host:, port:)
      @id = id
      @host = host
      @port = port
    end
  end

  class MetadataPartition
    def self.from_native(p)
      n = Rdkafka::Bindings::MetadataPartition.new(p)
      replicas = n[:replicas].read_array_of_type(:int32, :read_int32, n[:replica_cnt])
      isrs = n[:isrs].read_array_of_type(:int32, :read_int32, n[:isr_cnt])
      MetadataPartition.new(
        id: n[:id],
        err: n[:err],
        leader: n[:leader],
        replicas: replicas,
        isrs: isrs,
      )
    end

    attr_reader :id, :err, :leader, :replicas, :isrs

    def initialize(id:, err:, leader:, replicas:, isrs:)
      @id = id
      @err = err
      @leader = leader
      @replicas = replicas
      @isrs = isrs
    end
  end

  class MetadataTopic
    def self.from_native(p)
      n = Rdkafka::Bindings::MetadataTopic.new(p)

      partitions = n[:partition_cnt].times.map do |i|
        MetadataPartition.from_native(n[:partitions][i * Rdkafka::Bindings::MetadataPartition.size])
      end

      MetadataTopic.new(
        topic: n[:topic],
        partitions: partitions,
        err: n[:err]
      )
    end

    attr_reader :topic, :partitions, :err

    def initialize(topic:, partitions:, err:)
      @topic = topic
      @partitions = partitions
      @err = err
    end
  end

  class Metadata
    def self.from_native(p)
      n = Rdkafka::Bindings::Metadata.new(p)

      brokers = n[:broker_cnt].times.map do |i|
        MetadataBroker.from_native(n[:brokers][i * Rdkafka::Bindings::MetadataBroker.size])
      end
      topics = n[:topic_cnt].times.map do |i|
        MetadataTopic.from_native(n[:topics][i * Rdkafka::Bindings::MetadataTopic.size])
      end

      Metadata.new(
        brokers: brokers,
        topics: topics
      )
    end

    attr_reader :brokers, :topics

    def initialize(brokers:, topics:)
      @brokers = brokers
      @topics = topics
    end
  end

end
