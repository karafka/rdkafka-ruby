# frozen_string_literal: true

require "ostruct"
require "securerandom"

describe Rdkafka::Consumer do
  let(:consumer) { rdkafka_consumer_config.consumer }
  let(:producer) { rdkafka_producer_config.producer }
  let(:topic) { TestTopics.consume_test_topic }

  before do
    @consumer = consumer
    @producer = producer
  end

  after do
    @consumer&.close
    @producer&.close
  end

  # -- #name --

  describe "#name" do
    it "includes consumer prefix" do
      assert_includes @consumer.name, "rdkafka#consumer-"
    end
  end

  # -- consumer without auto-start --

  describe "consumer without auto-start" do
    it "can start later and close" do
      consumer = rdkafka_consumer_config.consumer(native_kafka_auto_start: false)
      consumer.start
      consumer.close
    ensure
      consumer&.close
    end

    it "can close without starting" do
      consumer = rdkafka_consumer_config.consumer(native_kafka_auto_start: false)
      consumer.close
    ensure
      consumer&.close
    end
  end

  # -- #subscribe, #unsubscribe and #subscription --

  describe "#subscribe, #unsubscribe and #subscription" do
    it "subscribes, unsubscribes and returns the subscription" do
      assert_empty @consumer.subscription

      @consumer.subscribe(topic)

      refute_empty @consumer.subscription
      expected_subscription = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic(topic)
      end

      assert_equal expected_subscription, @consumer.subscription

      @consumer.unsubscribe

      assert_empty @consumer.subscription
    end

    it "raises an error when subscribing fails" do
      Rdkafka::Bindings.stub(:rd_kafka_subscribe, 20) do
        assert_raises(Rdkafka::RdkafkaError) do
          @consumer.subscribe(topic)
        end
      end
    end

    it "raises an error when unsubscribing fails" do
      Rdkafka::Bindings.stub(:rd_kafka_unsubscribe, 20) do
        assert_raises(Rdkafka::RdkafkaError) do
          @consumer.unsubscribe
        end
      end
    end

    it "raises an error when fetching the subscription fails" do
      Rdkafka::Bindings.stub(:rd_kafka_subscription, 20) do
        assert_raises(Rdkafka::RdkafkaError) do
          @consumer.subscription
        end
      end
    end

    describe "without poll set" do
      it "subscribes, unsubscribes and returns the subscription" do
        config = rdkafka_consumer_config
        config.consumer_poll_set = false
        consumer = config.consumer

        assert_empty consumer.subscription

        consumer.subscribe(topic)

        refute_empty consumer.subscription
        expected_subscription = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
          list.add_topic(topic)
        end

        assert_equal expected_subscription, consumer.subscription

        consumer.unsubscribe

        assert_empty consumer.subscription
      ensure
        consumer&.close
      end
    end
  end

  # -- #pause and #resume --

  describe "#pause and #resume" do
    describe "subscription" do
      it "pauses and then resumes" do
        timeout = 2000
        @consumer.subscribe(topic)

        # 1. partitions are assigned
        wait_for_assignment(@consumer)

        refute_empty @consumer.assignment

        # 2. send a first message
        @producer.produce(
          topic: topic,
          payload: "payload 1",
          key: "key 1"
        ).wait

        # 3. ensure that message is successfully consumed
        records = @consumer.poll(timeout)

        refute_nil records
        @consumer.commit

        # 4. send a second message
        @producer.produce(
          topic: topic,
          payload: "payload 1",
          key: "key 1"
        ).wait

        # 5. pause the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic(topic, 0..2)
        @consumer.pause(tpl)

        # 6. ensure that messages are not available
        records = @consumer.poll(timeout)

        assert_nil records

        # 7. resume the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic(topic, 0..2)
        @consumer.resume(tpl)

        # 8. ensure that message is successfully consumed
        records = @consumer.poll(timeout)

        refute_nil records
      ensure
        @consumer.unsubscribe
      end
    end

    it "raises when not TopicPartitionList" do
      assert_raises(TypeError) { @consumer.pause(true) }
      assert_raises(TypeError) { @consumer.resume(true) }
    end

    it "raises an error when pausing fails" do
      list = Rdkafka::Consumer::TopicPartitionList.new.tap { |tpl| tpl.add_topic("topic", 0..1) }

      Rdkafka::Bindings.stub(:rd_kafka_pause_partitions, 20) do
        err = assert_raises(Rdkafka::RdkafkaTopicPartitionListError) do
          @consumer.pause(list)
        end
        refute_nil err.topic_partition_list
      end
    end

    it "raises an error when resume fails" do
      Rdkafka::Bindings.stub(:rd_kafka_resume_partitions, 20) do
        assert_raises(Rdkafka::RdkafkaError) do
          @consumer.resume(Rdkafka::Consumer::TopicPartitionList.new)
        end
      end
    end
  end

  # -- #seek --

  describe "#seek" do
    it "raises an error when seeking fails" do
      topic = "it-#{SecureRandom.uuid}"
      admin = rdkafka_producer_config.admin
      admin.create_topic(topic, 1, 1).wait
      wait_for_topic(admin, topic)
      admin.close

      fake_msg = OpenStruct.new(topic: topic, partition: 0, offset: 0)

      Rdkafka::Bindings.stub(:rd_kafka_seek, 20) do
        assert_raises(Rdkafka::RdkafkaError) do
          @consumer.seek(fake_msg)
        end
      end
    end

    it "works when a partition is paused" do
      topic = "it-#{SecureRandom.uuid}"
      admin = rdkafka_producer_config.admin
      admin.create_topic(topic, 1, 1).wait
      wait_for_topic(admin, topic)
      admin.close

      timeout = 1000
      consumer = rdkafka_consumer_config("auto.commit.interval.ms": 60_000).consumer
      consumer.subscribe(topic)

      # 1. partitions are assigned
      wait_for_assignment(consumer)

      refute_empty consumer.assignment

      # 2. eat unrelated messages
      while consumer.poll(timeout) do; end

      # 3. get reference message
      @producer.produce(
        topic: topic,
        payload: "payload a",
        key: "key 1",
        partition: 0
      ).wait
      message1 = consumer.poll(timeout)

      assert_equal "payload a", message1&.payload

      # 4. pause the subscription
      tpl = Rdkafka::Consumer::TopicPartitionList.new
      tpl.add_topic(topic, 1)
      consumer.pause(tpl)

      # 5. seek to previous message
      consumer.seek(message1)

      # 6. resume the subscription
      tpl = Rdkafka::Consumer::TopicPartitionList.new
      tpl.add_topic(topic, 1)
      consumer.resume(tpl)

      # 7. ensure same message is read again
      message2 = consumer.poll(timeout)

      # This is needed because `enable.auto.offset.store` is true but when running in CI that
      # is overloaded, offset store lags
      sleep(1)

      consumer.commit

      assert_equal message1.offset, message2.offset
      assert_equal message1.payload, message2.payload
    ensure
      consumer&.unsubscribe
      consumer&.close
    end

    it "allows skipping messages" do
      topic = "it-#{SecureRandom.uuid}"
      admin = rdkafka_producer_config.admin
      admin.create_topic(topic, 1, 1).wait
      wait_for_topic(admin, topic)
      admin.close

      timeout = 1000
      consumer = rdkafka_consumer_config("auto.commit.interval.ms": 60_000).consumer
      consumer.subscribe(topic)

      # 1. partitions are assigned
      wait_for_assignment(consumer)

      refute_empty consumer.assignment

      # 2. eat unrelated messages
      while consumer.poll(timeout) do; end

      # 3. send messages
      [:a, :b, :c].each do |val|
        @producer.produce(
          topic: topic,
          payload: "payload #{val}",
          key: "key 1",
          partition: 0
        ).wait
      end

      # 4. get reference message
      message = consumer.poll(timeout)

      assert_equal "payload a", message&.payload

      # 5. seek over one message
      fake_msg = message.dup
      fake_msg.instance_variable_set(:@offset, fake_msg.offset + 2)
      consumer.seek(fake_msg)

      # 6. ensure that only one message is available
      records = consumer.poll(timeout)

      assert_equal "payload c", records&.payload
      records = consumer.poll(timeout)

      assert_nil records
    ensure
      consumer&.unsubscribe
      consumer&.close
    end
  end

  # -- #seek_by --

  describe "#seek_by" do
    it "raises an error when seeking fails" do
      topic = "it-#{SecureRandom.uuid}"
      admin = rdkafka_producer_config.admin
      admin.create_topic(topic, 1, 1).wait
      wait_for_topic(admin, topic)
      admin.close

      Rdkafka::Bindings.stub(:rd_kafka_seek, 20) do
        assert_raises(Rdkafka::RdkafkaError) do
          @consumer.seek_by(topic, 0, 0)
        end
      end
    end

    it "works when a partition is paused" do
      topic = "it-#{SecureRandom.uuid}"
      admin = rdkafka_producer_config.admin
      admin.create_topic(topic, 1, 1).wait
      wait_for_topic(admin, topic)
      admin.close

      timeout = 1000
      consumer = rdkafka_consumer_config("auto.commit.interval.ms": 60_000).consumer
      consumer.subscribe(topic)

      # 1. partitions are assigned
      wait_for_assignment(consumer)

      refute_empty consumer.assignment

      # 2. eat unrelated messages
      while consumer.poll(timeout) do; end

      # 3. get reference message
      @producer.produce(
        topic: topic,
        payload: "payload a",
        key: "key 1",
        partition: 0
      ).wait
      message1 = consumer.poll(timeout)

      assert_equal "payload a", message1&.payload

      # 4. pause the subscription
      tpl = Rdkafka::Consumer::TopicPartitionList.new
      tpl.add_topic(topic, 1)
      consumer.pause(tpl)

      # 5. seek by the previous message fields
      consumer.seek_by(message1.topic, message1.partition, message1.offset)

      # 6. resume the subscription
      tpl = Rdkafka::Consumer::TopicPartitionList.new
      tpl.add_topic(topic, 1)
      consumer.resume(tpl)

      # 7. ensure same message is read again
      message2 = consumer.poll(timeout)

      # This is needed because `enable.auto.offset.store` is true but when running in CI that
      # is overloaded, offset store lags
      sleep(2)

      consumer.commit

      assert_equal message1.offset, message2.offset
      assert_equal message1.payload, message2.payload
    ensure
      consumer&.unsubscribe
      consumer&.close
    end

    it "allows skipping messages" do
      topic = "it-#{SecureRandom.uuid}"
      admin = rdkafka_producer_config.admin
      admin.create_topic(topic, 1, 1).wait
      wait_for_topic(admin, topic)
      admin.close

      timeout = 1000
      consumer = rdkafka_consumer_config("auto.commit.interval.ms": 60_000).consumer
      consumer.subscribe(topic)

      # 1. partitions are assigned
      wait_for_assignment(consumer)

      refute_empty consumer.assignment

      # 2. eat unrelated messages
      while consumer.poll(timeout) do; end

      # 3. send messages
      [:a, :b, :c].each do |val|
        @producer.produce(
          topic: topic,
          payload: "payload #{val}",
          key: "key 1",
          partition: 0
        ).wait
      end

      # 4. get reference message
      message = consumer.poll(timeout)

      assert_equal "payload a", message&.payload

      # 5. seek over one message
      consumer.seek_by(message.topic, message.partition, message.offset + 2)

      # 6. ensure that only one message is available
      records = consumer.poll(timeout)

      assert_equal "payload c", records&.payload
      records = consumer.poll(timeout)

      assert_nil records
    ensure
      consumer&.unsubscribe
      consumer&.close
    end
  end

  # -- #assign and #assignment --

  describe "#assign and #assignment" do
    it "returns an empty assignment if nothing is assigned" do
      assert_empty @consumer.assignment
    end

    it "only accepts a topic partition list" do
      assert_raises(TypeError) do
        @consumer.assign("list")
      end
    end

    it "raises an error when assigning fails" do
      Rdkafka::Bindings.stub(:rd_kafka_assign, 20) do
        assert_raises(Rdkafka::RdkafkaError) do
          @consumer.assign(Rdkafka::Consumer::TopicPartitionList.new)
        end
      end
    end

    it "assigns specific topic partitions and returns that assignment" do
      tpl = Rdkafka::Consumer::TopicPartitionList.new
      tpl.add_topic(topic, 0..2)
      @consumer.assign(tpl)

      assignment = @consumer.assignment

      refute_empty assignment
      assert_equal 3, assignment.to_h[topic].length
    end

    it "returns the assignment when subscribed" do
      # Make sure there's a message
      @producer.produce(
        topic: topic,
        payload: "payload 1",
        key: "key 1",
        partition: 0
      ).wait

      # Subscribe and poll until partitions are assigned
      @consumer.subscribe(topic)
      100.times do
        @consumer.poll(100)
        break unless @consumer.assignment.empty?
      end

      assignment = @consumer.assignment

      refute_empty assignment
      assert_equal 3, assignment.to_h[topic].length
    end

    it "raises an error when getting assignment fails" do
      Rdkafka::Bindings.stub(:rd_kafka_assignment, 20) do
        assert_raises(Rdkafka::RdkafkaError) do
          @consumer.assignment
        end
      end
    end
  end

  # -- #assignment_lost? --

  describe "#assignment_lost?" do
    it "does not return true when we have an assignment" do
      @consumer.subscribe(topic)
      Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic(topic)
      end

      refute_predicate @consumer, :assignment_lost?
      @consumer.unsubscribe
    end

    it "does not return true after voluntary unsubscribing" do
      @consumer.subscribe(topic)
      Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic(topic)
      end

      @consumer.unsubscribe

      refute_predicate @consumer, :assignment_lost?
    end
  end

  # -- #close --

  describe "#close" do
    it "closes a consumer" do
      @consumer.subscribe(topic)
      100.times do |i|
        @producer.produce(
          topic: topic,
          payload: "payload #{i}",
          key: "key #{i}",
          partition: 0
        ).wait
      end
      @consumer.close
      error = assert_raises(Rdkafka::ClosedConsumerError) do
        @consumer.poll(100)
      end
      assert_match(/poll/, error.message)
    end

    it "waits for outgoing operations in other threads" do
      times = []
      # Eagerly evaluate topic before spawning thread so the main thread
      # doesn't close the consumer before subscribe completes
      topic_name = topic

      # Run a long running poll
      thread = Thread.new do
        times << Time.now
        @consumer.subscribe(topic_name)
        times << Time.now
        @consumer.poll(1_000)
        times << Time.now
      end

      # Make sure it starts before we close
      sleep(0.1)
      @consumer.close
      close_time = Time.now
      thread.join

      times.each do |t|
        assert_operator t, :<, close_time
      end
    end
  end

  # -- #position, #commit, #committed and #store_offset --

  describe "#position, #commit, #committed and #store_offset" do
    it "position only accepts a topic partition list if not nil" do
      assert_raises(TypeError) do
        @consumer.position("list")
      end
    end

    it "committed only accepts a topic partition list if not nil" do
      assert_raises(TypeError) do
        @consumer.commit("list")
      end
    end

    it "commits in sync mode" do
      @consumer.commit(nil, true)
    end

    it "commits a specific topic partition list" do
      # Make sure there are some messages
      handles = []
      10.times do
        (0..2).each do |i|
          handles << @producer.produce(
            topic: topic,
            payload: "payload 1",
            key: "key 1",
            partition: i
          )
        end
      end
      handles.each(&:wait)

      @consumer.subscribe(topic)
      wait_for_assignment(@consumer)
      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic_and_partitions_with_offsets(topic, 0 => 1, 1 => 1, 2 => 1)
      end
      @consumer.commit(list)

      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic_and_partitions_with_offsets(topic, 0 => 1, 1 => 2, 2 => 3)
      end
      @consumer.commit(list)

      partitions = @consumer.committed(list).to_h[topic]

      assert_equal 1, partitions[0].offset
      assert_equal 2, partitions[1].offset
      assert_equal 3, partitions[2].offset
    end

    it "raises an error when committing fails" do
      # Need to subscribe and set up committed state first
      handles = []
      10.times do
        (0..2).each do |i|
          handles << @producer.produce(
            topic: topic,
            payload: "payload 1",
            key: "key 1",
            partition: i
          )
        end
      end
      handles.each(&:wait)

      @consumer.subscribe(topic)
      wait_for_assignment(@consumer)
      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic_and_partitions_with_offsets(topic, 0 => 1, 1 => 1, 2 => 1)
      end
      @consumer.commit(list)

      Rdkafka::Bindings.stub(:rd_kafka_commit, 20) do
        assert_raises(Rdkafka::RdkafkaError) do
          @consumer.commit
        end
      end
    end

    it "fetches the committed offsets for the current assignment" do
      # Setup committed state
      handles = []
      10.times do
        (0..2).each do |i|
          handles << @producer.produce(
            topic: topic,
            payload: "payload 1",
            key: "key 1",
            partition: i
          )
        end
      end
      handles.each(&:wait)

      @consumer.subscribe(topic)
      wait_for_assignment(@consumer)
      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic_and_partitions_with_offsets(topic, 0 => 1, 1 => 1, 2 => 1)
      end
      @consumer.commit(list)

      partitions = @consumer.committed.to_h[topic]

      refute_nil partitions
      assert_equal 1, partitions[0].offset
    end

    it "fetches the committed offsets for a specified topic partition list" do
      # Setup committed state
      handles = []
      10.times do
        (0..2).each do |i|
          handles << @producer.produce(
            topic: topic,
            payload: "payload 1",
            key: "key 1",
            partition: i
          )
        end
      end
      handles.each(&:wait)

      @consumer.subscribe(topic)
      wait_for_assignment(@consumer)
      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic_and_partitions_with_offsets(topic, 0 => 1, 1 => 1, 2 => 1)
      end
      @consumer.commit(list)

      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic(topic, [0, 1, 2])
      end
      partitions = @consumer.committed(list).to_h[topic]

      refute_nil partitions
      assert_equal 1, partitions[0].offset
      assert_equal 1, partitions[1].offset
      assert_equal 1, partitions[2].offset
    end

    it "raises an error when getting committed fails" do
      Rdkafka::Bindings.stub(:rd_kafka_committed, 20) do
        list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
          l.add_topic(topic, [0, 1, 2])
        end
        assert_raises(Rdkafka::RdkafkaError) do
          @consumer.committed(list)
        end
      end
    end
  end

  # -- #store_offset --

  describe "#store_offset" do
    it "stores the offset for a message" do
      report = @producer.produce(
        topic: topic,
        payload: "payload 1",
        key: "key 1",
        partition: 0
      ).wait

      message = wait_for_message(
        topic: topic,
        delivery_report: report,
        consumer: @consumer
      )

      group_id = SecureRandom.uuid
      base_config = {
        "group.id": group_id,
        "enable.auto.offset.store": false,
        "enable.auto.commit": false
      }

      new_consumer = rdkafka_consumer_config(base_config).consumer
      new_consumer.subscribe(topic)
      wait_for_assignment(new_consumer)

      new_consumer.store_offset(message)
      new_consumer.commit

      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic(topic, [0, 1, 2])
      end
      partitions = new_consumer.committed(list).to_h[topic]

      refute_nil partitions
      assert_equal(message.offset + 1, partitions[message.partition].offset)
    ensure
      new_consumer&.close
    end

    it "raises an error with invalid input" do
      report = @producer.produce(
        topic: topic,
        payload: "payload 1",
        key: "key 1",
        partition: 0
      ).wait

      message = wait_for_message(
        topic: topic,
        delivery_report: report,
        consumer: @consumer
      )

      group_id = SecureRandom.uuid
      base_config = {
        "group.id": group_id,
        "enable.auto.offset.store": false,
        "enable.auto.commit": false
      }

      new_consumer = rdkafka_consumer_config(base_config).consumer
      new_consumer.subscribe(topic)
      wait_for_assignment(new_consumer)

      message.stub(:partition, 9999) do
        assert_raises(Rdkafka::RdkafkaError) do
          new_consumer.store_offset(message)
        end
      end
    ensure
      new_consumer&.close
    end

    it "raises error when auto offset store is enabled" do
      consumer = rdkafka_consumer_config("enable.auto.offset.store": true).consumer

      report = @producer.produce(
        topic: topic,
        payload: "payload 1",
        key: "key 1",
        partition: 0
      ).wait

      message = wait_for_message(
        topic: topic,
        delivery_report: report,
        consumer: consumer
      )

      # Setup committed state
      consumer.subscribe(topic)
      wait_for_assignment(consumer)
      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic_and_partitions_with_offsets(topic, 0 => 1, 1 => 1, 2 => 1)
      end
      consumer.commit(list)

      error = assert_raises(Rdkafka::RdkafkaError) do
        consumer.store_offset(message)
      end
      assert_match(/invalid_arg/, error.message)
    ensure
      consumer&.close
    end
  end

  # -- #position --

  describe "#position" do
    it "fetches the positions for the current assignment" do
      consumer = rdkafka_consumer_config("enable.auto.offset.store": false).consumer

      report = @producer.produce(
        topic: topic,
        payload: "payload 1",
        key: "key 1",
        partition: 0
      ).wait

      message = wait_for_message(
        topic: topic,
        delivery_report: report,
        consumer: consumer
      )

      # Consumer is already subscribed and assigned from wait_for_message
      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic_and_partitions_with_offsets(topic, 0 => 1, 1 => 1, 2 => 1)
      end
      consumer.commit(list)

      consumer.store_offset(message)

      partitions = consumer.position.to_h[topic]

      refute_nil partitions
      assert_equal message.offset + 1, partitions[0].offset
    ensure
      consumer&.close
    end

    it "fetches the positions for a specified assignment" do
      consumer = rdkafka_consumer_config("enable.auto.offset.store": false).consumer

      report = @producer.produce(
        topic: topic,
        payload: "payload 1",
        key: "key 1",
        partition: 0
      ).wait

      message = wait_for_message(
        topic: topic,
        delivery_report: report,
        consumer: consumer
      )

      # Consumer is already subscribed and assigned from wait_for_message
      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic_and_partitions_with_offsets(topic, 0 => 1, 1 => 1, 2 => 1)
      end
      consumer.commit(list)

      consumer.store_offset(message)

      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic_and_partitions_with_offsets(topic, 0 => nil, 1 => nil, 2 => nil)
      end
      partitions = consumer.position(list).to_h[topic]

      refute_nil partitions
      assert_equal message.offset + 1, partitions[0].offset
    ensure
      consumer&.close
    end

    it "raises an error when getting the position fails" do
      consumer = rdkafka_consumer_config("enable.auto.offset.store": false).consumer

      # Setup committed state
      handles = []
      10.times do
        (0..2).each do |i|
          handles << @producer.produce(
            topic: topic,
            payload: "payload 1",
            key: "key 1",
            partition: i
          )
        end
      end
      handles.each(&:wait)

      consumer.subscribe(topic)
      wait_for_assignment(consumer)
      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic_and_partitions_with_offsets(topic, 0 => 1, 1 => 1, 2 => 1)
      end
      consumer.commit(list)

      Rdkafka::Bindings.stub(:rd_kafka_position, 20) do
        assert_raises(Rdkafka::RdkafkaError) do
          consumer.position
        end
      end
    ensure
      consumer&.close
    end
  end

  # -- #query_watermark_offsets --

  describe "#query_watermark_offsets" do
    it "returns the watermark offsets" do
      # Make sure there's a message
      @producer.produce(
        topic: topic,
        payload: "payload 1",
        key: "key 1",
        partition: 0
      ).wait

      low, high = @consumer.query_watermark_offsets(topic, 0, 5000)

      assert_equal 0, low
      assert_operator high, :>, 0
    end

    it "raises an error when querying fails" do
      Rdkafka::Bindings.stub(:rd_kafka_query_watermark_offsets, 20) do
        assert_raises(Rdkafka::RdkafkaError) do
          @consumer.query_watermark_offsets(topic, 0, 5000)
        end
      end
    end
  end

  # -- #lag --

  describe "#lag" do
    it "calculates the consumer lag" do
      consumer = rdkafka_consumer_config("enable.partition.eof": true).consumer

      # Make sure there's a message in every partition and
      # wait for the message to make sure everything is committed.
      (0..2).each do |i|
        @producer.produce(
          topic: topic,
          key: "key lag #{i}",
          partition: i
        ).wait
      end

      # Consume to the end
      consumer.subscribe(topic)
      wait_for_assignment(consumer)
      message_count = 0
      loop do
        begin
          message = consumer.poll(1000)
          message_count += 1 if message
        rescue Rdkafka::RdkafkaError => error
          raise unless error.is_partition_eof?
        end
        break if message_count >= 3
      end

      # Commit
      consumer.commit

      # Create list to fetch lag for
      list = consumer.committed(Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic(topic, 0..2)
      end)

      # Lag should be 0 now
      lag = consumer.lag(list)
      expected_lag = {
        topic => {
          0 => 0,
          1 => 0,
          2 => 0
        }
      }

      assert_equal expected_lag, lag

      # Produce message on every topic again
      (0..2).each do |i|
        @producer.produce(
          topic: topic,
          key: "key lag #{i}",
          partition: i
        ).wait
      end

      # Lag should be 1 now
      lag = consumer.lag(list)
      expected_lag = {
        topic => {
          0 => 1,
          1 => 1,
          2 => 1
        }
      }

      assert_equal expected_lag, lag
    ensure
      consumer&.close
    end

    it "returns nil if there are no messages on the topic" do
      consumer = rdkafka_consumer_config("enable.partition.eof": true).consumer

      # Subscribe to establish the group coordinator before calling committed
      consumer.subscribe(topic)
      wait_for_assignment(consumer)

      list = consumer.committed(Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic(topic, 0..2)
      end)

      lag = consumer.lag(list)
      expected_lag = {
        topic => {}
      }

      assert_equal expected_lag, lag
    ensure
      consumer&.close
    end
  end

  # -- #cluster_id --

  describe "#cluster_id" do
    it "returns the current cluster id" do
      @consumer.subscribe(topic)
      wait_for_assignment(@consumer)

      refute_empty @consumer.cluster_id
    end
  end

  # -- #member_id --

  describe "#member_id" do
    it "returns the current member id" do
      @consumer.subscribe(topic)
      wait_for_assignment(@consumer)

      assert @consumer.member_id.start_with?("rdkafka-")
    end
  end

  # -- #poll --

  describe "#poll" do
    it "returns nil if there is no subscription" do
      assert_nil @consumer.poll(1000)
    end

    it "returns nil if there are no messages" do
      @consumer.subscribe(topic)

      assert_nil @consumer.poll(1000)
    end

    it "returns a message if there is one" do
      topic = "it-#{SecureRandom.uuid}"

      @producer.produce(
        topic: topic,
        payload: "payload 1",
        key: "key 1"
      ).wait
      @consumer.subscribe(topic)
      message = @consumer.each { |m| break m }

      assert_kind_of Rdkafka::Consumer::Message, message
      assert_equal "payload 1", message.payload
      assert_equal "key 1", message.key
    end

    it "raises an error when polling fails" do
      message = Rdkafka::Bindings::Message.new.tap do |msg|
        msg[:err] = 20
      end
      message_pointer = message.to_ptr

      Rdkafka::Bindings.stub(:rd_kafka_consumer_poll, message_pointer) do
        Rdkafka::Bindings.stub(:rd_kafka_message_destroy, nil) do
          assert_raises(Rdkafka::RdkafkaError) do
            @consumer.poll(100)
          end
        end
      end
    end
  end

  # -- #poll_nb --

  describe "#poll_nb" do
    it "returns nil if there is no subscription" do
      assert_nil @consumer.poll_nb
    end

    it "returns nil if there are no messages" do
      @consumer.subscribe(topic)

      assert_nil @consumer.poll_nb
    end

    it "accepts a timeout parameter" do
      @consumer.subscribe(topic)

      assert_nil @consumer.poll_nb(0)
      assert_nil @consumer.poll_nb(100)
    end

    it "returns a message if there is one" do
      topic = "it-#{SecureRandom.uuid}"

      @producer.produce(
        topic: topic,
        payload: "payload poll_nb",
        key: "key poll_nb"
      ).wait

      @consumer.subscribe(topic)
      wait_for_assignment(@consumer)

      # Give time for message to arrive
      sleep 1

      message = nil
      10.times do
        message = @consumer.poll_nb(100)
        break if message
        sleep 0.1
      end

      assert_kind_of Rdkafka::Consumer::Message, message
      assert_equal "payload poll_nb", message.payload
      assert_equal "key poll_nb", message.key
    end

    it "raises an error when polling fails" do
      message = Rdkafka::Bindings::Message.new.tap do |msg|
        msg[:err] = 20
      end
      message_pointer = message.to_ptr

      Rdkafka::Bindings.stub(:rd_kafka_consumer_poll_nb, message_pointer) do
        Rdkafka::Bindings.stub(:rd_kafka_message_destroy, nil) do
          assert_raises(Rdkafka::RdkafkaError) do
            @consumer.poll_nb
          end
        end
      end
    end

    it "raises closed consumer error when closed" do
      @consumer.close
      error = assert_raises(Rdkafka::ClosedConsumerError) do
        @consumer.poll_nb
      end
      assert_match(/poll_nb/, error.message)
    end
  end

  # -- #poll with headers --

  describe "#poll with headers" do
    it "returns message with headers using symbol keys" do
      report = @producer.produce(
        topic: topic,
        key: "key headers",
        headers: { foo: "bar" }
      ).wait

      message = wait_for_message(topic: topic, consumer: @consumer, delivery_report: report)

      refute_nil message
      assert_equal "key headers", message.key
      assert_includes message.headers, "foo"
      assert_equal "bar", message.headers["foo"]
    end

    it "returns message with headers using string keys" do
      report = @producer.produce(
        topic: topic,
        key: "key headers",
        headers: { "foo" => "bar" }
      ).wait

      message = wait_for_message(topic: topic, consumer: @consumer, delivery_report: report)

      refute_nil message
      assert_equal "key headers", message.key
      assert_includes message.headers, "foo"
      assert_equal "bar", message.headers["foo"]
    end

    it "returns message with no headers" do
      report = @producer.produce(
        topic: topic,
        key: "key no headers",
        headers: nil
      ).wait

      message = wait_for_message(topic: topic, consumer: @consumer, delivery_report: report)

      refute_nil message
      assert_equal "key no headers", message.key
      assert_empty message.headers
    end

    it "raises an error when message headers are not readable" do
      report = @producer.produce(
        topic: topic,
        key: "key err headers",
        headers: nil
      ).wait

      Rdkafka::Bindings.stub(:rd_kafka_message_headers, 1) do
        err = assert_raises(Rdkafka::RdkafkaError) do
          wait_for_message(topic: topic, consumer: @consumer, delivery_report: report)
        end
        assert err.message.start_with?("Error reading message headers")
      end
    end

    it "raises an error when the first message header is not readable" do
      report = @producer.produce(
        topic: topic,
        key: "key err headers",
        headers: { foo: "bar" }
      ).wait

      Rdkafka::Bindings.stub(:rd_kafka_header_get_all, 1) do
        err = assert_raises(Rdkafka::RdkafkaError) do
          wait_for_message(topic: topic, consumer: @consumer, delivery_report: report)
        end
        assert err.message.start_with?("Error reading a message header at index 0")
      end
    end
  end

  # -- #each --

  describe "#each" do
    it "yields messages" do
      handles = []
      10.times do
        handles << @producer.produce(
          topic: topic,
          payload: "payload 1",
          key: "key 1",
          partition: 0
        )
      end
      handles.each(&:wait)

      @consumer.subscribe(topic)
      # Check the first 10 messages. Then close the consumer, which
      # should break the each loop.
      @consumer.each_with_index do |message, i|
        assert_kind_of Rdkafka::Consumer::Message, message
        break if i == 9
      end
      @consumer.close
    end
  end

  # -- #each_batch --

  describe "#each_batch" do
    it "raises not implemented error" do
      assert_raises(NotImplementedError) do
        @consumer.each_batch {}
      end
    end
  end

  # -- #offsets_for_times --

  describe "#offsets_for_times" do
    it "raises when not TopicPartitionList" do
      assert_raises(TypeError) { @consumer.offsets_for_times([]) }
    end

    it "raises an error when offsets_for_times fails" do
      tpl = Rdkafka::Consumer::TopicPartitionList.new

      Rdkafka::Bindings.stub(:rd_kafka_offsets_for_times, 7) do
        assert_raises(Rdkafka::RdkafkaError) { @consumer.offsets_for_times(tpl) }
      end
    end

    it "returns a TopicPartitionList with updated offsets" do
      timeout = 1000

      @consumer.subscribe(topic)

      # 1. partitions are assigned
      wait_for_assignment(@consumer)

      refute_empty @consumer.assignment

      # 2. eat unrelated messages
      while @consumer.poll(timeout) do; end

      # 3. send messages
      [:a, :b, :c].each do |val|
        @producer.produce(
          topic: topic,
          payload: "payload #{val}",
          key: "key 0",
          partition: 0
        ).wait
      end

      @consumer.poll(timeout)
      message = @consumer.poll(timeout)
      @consumer.poll(timeout)

      tpl = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic_and_partitions_with_offsets(
          topic,
          [
            [0, message.timestamp]
          ]
        )
      end

      tpl_response = @consumer.offsets_for_times(tpl)

      assert_equal message.offset, tpl_response.to_h[topic][0].offset
    ensure
      @consumer.unsubscribe
    end
  end

  # -- #events_poll --

  describe "#events_poll" do
    it "propagates stats on events_poll and not poll" do
      # Close the producer from setup so its background stats don't interfere
      @producer.close
      @producer = nil

      stats = []
      config = rdkafka_consumer_config("statistics.interval.ms": 500)
      config.consumer_poll_set = false
      consumer = config.consumer

      Rdkafka::Config.statistics_callback = ->(published) { stats << published }

      consumer.subscribe(topic)
      consumer.poll(1_000)

      assert_empty stats
      consumer.events_poll(-1)

      refute_empty stats
    ensure
      Rdkafka::Config.statistics_callback = nil
      consumer&.close
    end
  end

  # -- #events_poll_nb --

  describe "#events_poll_nb" do
    it "returns the number of events processed" do
      config = rdkafka_consumer_config("statistics.interval.ms": 500)
      config.consumer_poll_set = false
      consumer = config.consumer

      Rdkafka::Config.statistics_callback = ->(published) {}

      consumer.subscribe(topic)
      result = consumer.events_poll_nb

      assert_kind_of Integer, result
      assert_operator result, :>=, 0
    ensure
      Rdkafka::Config.statistics_callback = nil
      consumer&.close
    end

    it "accepts a timeout parameter" do
      config = rdkafka_consumer_config("statistics.interval.ms": 500)
      config.consumer_poll_set = false
      consumer = config.consumer

      Rdkafka::Config.statistics_callback = ->(published) {}

      consumer.subscribe(topic)

      assert_operator consumer.events_poll_nb(0), :>=, 0
      assert_operator consumer.events_poll_nb(100), :>=, 0
    ensure
      Rdkafka::Config.statistics_callback = nil
      consumer&.close
    end

    it "processes events without releasing GVL" do
      # Close the producer from setup so its background stats don't interfere
      @producer.close
      @producer = nil

      stats = []
      config = rdkafka_consumer_config("statistics.interval.ms": 500)
      config.consumer_poll_set = false
      consumer = config.consumer

      Rdkafka::Config.statistics_callback = ->(published) { stats << published }

      consumer.subscribe(topic)
      consumer.poll(1_000)

      assert_empty stats

      # Wait for statistics to be ready
      sleep 0.6

      # Non-blocking poll should also process stats events
      consumer.events_poll_nb(100)

      refute_empty stats
    ensure
      Rdkafka::Config.statistics_callback = nil
      consumer&.close
    end
  end

  # -- #consumer_group_metadata_pointer --

  describe "#consumer_group_metadata_pointer" do
    it "returns a pointer" do
      pointer = @consumer.consumer_group_metadata_pointer

      assert_kind_of FFI::Pointer, pointer
    ensure
      Rdkafka::Bindings.rd_kafka_consumer_group_metadata_destroy(pointer) if pointer
    end
  end

  # -- a rebalance listener --

  describe "a rebalance listener" do
    it "gets notifications" do
      listener = Struct.new(:queue) do
        def on_partitions_assigned(list)
          collect(:assign, list)
        end

        def on_partitions_revoked(list)
          collect(:revoke, list)
        end

        def collect(name, list)
          partitions = list.to_h.map { |key, values| [key, values.map(&:partition)] }.flatten
          queue << ([name] + partitions)
        end
      end.new([])

      config = rdkafka_consumer_config
      config.consumer_rebalance_listener = listener
      @consumer.close
      @consumer = config.consumer

      notify_listener(@consumer, listener, topic: topic)
      @consumer = nil # notify_listener already closed the consumer

      assert_equal [
        [:assign, topic, 0, 1, 2],
        [:revoke, topic, 0, 1, 2]
      ], listener.queue
    end

    it "handles callback exceptions" do
      listener = Struct.new(:queue) do
        def on_partitions_assigned(list)
          queue << :assigned
          raise "boom"
        end

        def on_partitions_revoked(list)
          queue << :revoked
          raise "boom"
        end
      end.new([])

      config = rdkafka_consumer_config
      config.consumer_rebalance_listener = listener
      @consumer.close
      @consumer = config.consumer

      notify_listener(@consumer, listener, topic: topic)
      @consumer = nil # notify_listener already closed the consumer

      assert_equal [:assigned, :revoked], listener.queue
    end
  end

  # -- methods that should not be called after a consumer has been closed --

  describe "methods that should not be called after a consumer has been closed" do
    it "raises on subscribe" do
      @consumer.close
      error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.subscribe(nil) }
      assert_match(/subscribe/, error.message)
    end

    it "raises on unsubscribe" do
      @consumer.close
      error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.unsubscribe }
      assert_match(/unsubscribe/, error.message)
    end

    it "raises on pause" do
      @consumer.close
      error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.pause(nil) }
      assert_match(/pause/, error.message)
    end

    it "raises on resume" do
      @consumer.close
      error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.resume(nil) }
      assert_match(/resume/, error.message)
    end

    it "raises on subscription" do
      @consumer.close
      error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.subscription }
      assert_match(/subscription/, error.message)
    end

    it "raises on assign" do
      @consumer.close
      error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.assign(nil) }
      assert_match(/assign/, error.message)
    end

    it "raises on assignment" do
      @consumer.close
      error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.assignment }
      assert_match(/assignment/, error.message)
    end

    it "raises on committed" do
      @consumer.close
      error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.committed }
      assert_match(/committed/, error.message)
    end

    it "raises on query_watermark_offsets" do
      @consumer.close
      error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.query_watermark_offsets(nil, nil) }
      assert_match(/query_watermark_offsets/, error.message)
    end

    it "raises on assignment_lost?" do
      @consumer.close
      error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.assignment_lost? }
      assert_match(/assignment_lost\?/, error.message)
    end

    it "raises on poll_nb" do
      @consumer.close
      error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.poll_nb }
      assert_match(/poll_nb/, error.message)
    end
  end

  # -- finalizer --

  describe "finalizer" do
    it "provides a finalizer that closes the native kafka client" do
      refute_predicate @consumer, :closed?

      @consumer.finalizer.call("some-ignored-object-id")

      assert_predicate @consumer, :closed?
    end
  end

  # -- cooperative rebalance protocol --

  describe "when the rebalance protocol is cooperative" do
    it "assigns and unassigns partitions" do
      listener = Struct.new(:queue) do
        def on_partitions_assigned(list)
          collect(:assign, list)
        end

        def on_partitions_revoked(list)
          collect(:revoke, list)
        end

        def collect(name, list)
          partitions = list.to_h.map { |key, values| [key, values.map(&:partition)] }.flatten
          queue << ([name] + partitions)
        end
      end.new([])

      config = rdkafka_consumer_config(
        {
          "partition.assignment.strategy": "cooperative-sticky",
          debug: "consumer"
        }
      )
      config.consumer_rebalance_listener = listener
      @consumer.close
      @consumer = config.consumer

      # Produce messages before closing producer to avoid stats callback interference
      handles = []
      10.times do
        handles << @producer.produce(
          topic: topic,
          payload: "payload 1",
          key: "key 1",
          partition: 0
        )
      end
      handles.each(&:wait)
      @producer.close
      @producer = nil

      notify_listener(@consumer, listener, topic: topic) do
        @consumer.subscribe(topic)
        # Check the first 10 messages. Then close the consumer, which
        # should break the each loop.
        @consumer.each_with_index do |message, i|
          assert_kind_of Rdkafka::Consumer::Message, message
          break if i == 9
        end
      end
      @consumer = nil # notify_listener already closed the consumer

      assert_equal [
        [:assign, topic, 0, 1, 2],
        [:revoke, topic, 0, 1, 2]
      ], listener.queue
    end
  end

  # -- #oauthbearer_set_token --

  describe "#oauthbearer_set_token" do
    it "returns RD_KAFKA_RESP_ERR__STATE when sasl not configured" do
      response = @consumer.oauthbearer_set_token(
        token: "foo",
        lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
        principal_name: "kafka-cluster"
      )

      assert_equal Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE, response
    end

    it "succeeds when sasl configured without extensions" do
      consumer_sasl = rdkafka_producer_config(
        "security.protocol": "sasl_ssl",
        "sasl.mechanisms": "OAUTHBEARER"
      ).consumer

      response = consumer_sasl.oauthbearer_set_token(
        token: "foo",
        lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
        principal_name: "kafka-cluster"
      )

      assert_equal 0, response
    ensure
      consumer_sasl&.close
    end

    it "succeeds when sasl configured with extensions" do
      consumer_sasl = rdkafka_producer_config(
        "security.protocol": "sasl_ssl",
        "sasl.mechanisms": "OAUTHBEARER"
      ).consumer

      response = consumer_sasl.oauthbearer_set_token(
        token: "foo",
        lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
        principal_name: "kafka-cluster",
        extensions: {
          "foo" => "bar"
        }
      )

      assert_equal 0, response
    ensure
      consumer_sasl&.close
    end
  end

  # -- eof on topic with eof reporting enabled --

  describe "when reaching eof on a topic and eof reporting enabled" do
    it "returns proper details" do
      consumer = rdkafka_consumer_config("enable.partition.eof": true).consumer

      (0..2).each do |i|
        @producer.produce(
          topic: topic,
          key: "key lag #{i}",
          partition: i
        ).wait
      end

      # Consume to the end
      consumer.subscribe(topic)
      eof_error = nil

      loop do
        consumer.poll(100)
      rescue Rdkafka::RdkafkaError => error
        if error.is_partition_eof?
          eof_error = error
        end
        break if eof_error
      end

      assert_equal :partition_eof, eof_error.code
    ensure
      consumer&.close
    end
  end

  # -- long running consumption --

  describe "long running consumption" do
    it "consumes messages continuously for 60 seconds" do
      @consumer.subscribe(topic)
      wait_for_assignment(@consumer)

      messages_consumed = 0
      start_time = Time.now

      # Producer thread - sends message every second
      producer_thread = Thread.new do
        counter = 0
        while Time.now - start_time < 60
          @producer.produce(
            topic: topic,
            payload: "payload #{counter}",
            key: "key #{counter}",
            partition: 0
          ).wait
          counter += 1
          sleep(1)
        end
      end

      # Consumer loop
      while Time.now - start_time < 60
        message = @consumer.poll(1000)
        if message
          assert_kind_of Rdkafka::Consumer::Message, message
          assert_equal topic, message.topic
          messages_consumed += 1
          @consumer.commit if messages_consumed % 10 == 0
        end
      end

      producer_thread.join

      assert_operator messages_consumed, :>, 50 # Should consume most messages
    end
  end

  # -- #events_poll_nb_each --

  describe "#events_poll_nb_each" do
    it "does not raise when queue is empty" do
      @consumer.events_poll_nb_each { |_| }
    end

    it "yields the count after each poll" do
      counts = []
      call_count = 0
      stub_proc = proc do
        call_count += 1
        (call_count <= 2) ? 1 : 0
      end

      Rdkafka::Bindings.stub(:rd_kafka_poll_nb, stub_proc) do
        @consumer.events_poll_nb_each { |count| counts << count }
      end

      assert_equal [1, 1], counts
    end

    it "stops when block returns :stop" do
      iterations = 0

      Rdkafka::Bindings.stub(:rd_kafka_poll_nb, 1) do
        @consumer.events_poll_nb_each do |_count|
          iterations += 1
          :stop if iterations >= 3
        end
      end

      assert_equal 3, iterations
    end

    it "raises ClosedConsumerError when closed" do
      @consumer.close
      error = assert_raises(Rdkafka::ClosedConsumerError) do
        @consumer.events_poll_nb_each { |_| }
      end
      assert_match(/events_poll_nb_each/, error.message)
    end
  end

  # -- #poll_nb_each --

  describe "#poll_nb_each" do
    it "does not raise when queue is empty" do
      @consumer.subscribe(topic)
      # Give it a moment to subscribe
      sleep 0.5

      messages = []
      @consumer.poll_nb_each { |msg| messages << msg }

      assert_kind_of Array, messages
    end

    it "yields messages and respects :stop" do
      @consumer.subscribe(topic)

      # Produce some messages
      5.times { |i| @producer.produce(topic: topic, payload: "poll_nb_each test #{i}") }
      @producer.flush

      # Use blocking poll first to ensure consumer is ready and messages are fetched
      # poll_nb_each is non-blocking so we need to ensure messages are available first
      deadline = Time.now + 30
      first_message = nil
      while Time.now < deadline && first_message.nil?
        first_message = @consumer.poll(100)
      end

      # poll_nb_each is non-blocking, so we need messages in the local
      # fetch buffer. Keep doing blocking polls until we confirm more
      # messages are available, then put them back by seeking.
      # Alternatively, just wait for the fetch buffer to fill.
      messages = []
      deadline = Time.now + 30
      while Time.now < deadline && messages.empty?
        @consumer.poll_nb_each do |message|
          messages << message
          :stop if messages.size >= 1
        end
        sleep 0.1 if messages.empty?
      end

      # Should have stopped after exactly 1 message
      assert_equal 1, messages.size
    end

    it "properly cleans up message pointers" do
      @consumer.subscribe(topic)

      @producer.produce(topic: topic, payload: "cleanup test")
      @producer.flush
      sleep 2

      # This should not leak memory - message_destroy is called in ensure
      @consumer.poll_nb_each { |_| }
    end

    it "raises ClosedConsumerError when closed" do
      @consumer.close
      error = assert_raises(Rdkafka::ClosedConsumerError) do
        @consumer.poll_nb_each { |_| }
      end
      assert_match(/poll_nb_each/, error.message)
    end
  end

  # -- file descriptor access for fiber scheduler integration --

  describe "file descriptor access for fiber scheduler integration" do
    it "enables IO events on consumer queue" do
      @consumer.subscribe(topic)

      signal_r, signal_w = IO.pipe
      @consumer.enable_queue_io_events(signal_w.fileno)
      signal_r.close
      signal_w.close
    end

    it "enables IO events on background queue" do
      @consumer.subscribe(topic)

      signal_r, signal_w = IO.pipe
      @consumer.enable_background_queue_io_events(signal_w.fileno)
      signal_r.close
      signal_w.close
    end

    it "enables FD with payload option" do
      @consumer.subscribe(topic)

      signal_r, signal_w = IO.pipe
      custom_payload = "hello"
      @consumer.enable_queue_io_events(signal_w.fileno, custom_payload)
      signal_r.close
      signal_w.close
    end

    it "supports normal polling with IO events enabled" do
      @consumer.subscribe(topic)

      # Setup IO event signaling
      signal_r, signal_w = IO.pipe
      @consumer.enable_queue_io_events(signal_w.fileno)

      # Produce some messages
      @producer.produce(topic: topic, payload: "test message 1")
      @producer.produce(topic: topic, payload: "test message 2")
      @producer.flush

      # Give consumer time to rebalance and fetch
      sleep 2

      # Try to poll messages directly
      messages = []
      10.times do
        msg = @consumer.poll(100)
        messages << msg if msg
      end

      # We may or may not get messages depending on rebalancing, but should not error
      assert_kind_of Array, messages
      signal_r.close
      signal_w.close
    end

    describe "when consumer is closed" do
      it "raises ClosedInnerError on enable_queue_io_events" do
        @consumer.close
        signal_r, signal_w = IO.pipe
        assert_raises(Rdkafka::ClosedInnerError) do
          @consumer.enable_queue_io_events(signal_w.fileno)
        end
        signal_r.close
        signal_w.close
      end

      it "raises ClosedInnerError on enable_background_queue_io_events" do
        @consumer.close
        signal_r, signal_w = IO.pipe
        assert_raises(Rdkafka::ClosedInnerError) do
          @consumer.enable_background_queue_io_events(signal_w.fileno)
        end
        signal_r.close
        signal_w.close
      end
    end
  end
end
