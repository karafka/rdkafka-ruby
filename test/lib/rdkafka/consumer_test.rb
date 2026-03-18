# frozen_string_literal: true

require "ostruct"
require "securerandom"

require_relative "../../test_helper"

describe Rdkafka::Consumer do
  def consumer
    @consumer ||= rdkafka_consumer_config.consumer
  end

  def producer
    @producer ||= rdkafka_producer_config.producer
  end

  def topic
    @topic ||= TestTopics.create
  end

  after do
    consumer.close
    producer.close
  end

  describe "#name" do
    it "includes rdkafka#consumer-" do
      assert_includes consumer.name, "rdkafka#consumer-"
    end
  end

  describe "consumer without auto-start" do
    it "expect to be able to start it later and close" do
      @consumer = rdkafka_consumer_config.consumer(native_kafka_auto_start: false)
      consumer.start
      consumer.close
    end

    it "expect to be able to close it without starting" do
      @consumer = rdkafka_consumer_config.consumer(native_kafka_auto_start: false)
      consumer.close
    end
  end

  describe "#subscribe, #unsubscribe and #subscription" do
    it "subscribe,s unsubscribe and return the subscription" do
      assert_empty consumer.subscription

      consumer.subscribe(topic)

      refute_empty consumer.subscription
      expected_subscription = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic(topic)
      end
      assert_equal expected_subscription, consumer.subscription

      consumer.unsubscribe

      assert_empty consumer.subscription
    end

    it "raises an error when subscribing fails" do
      Rdkafka::Bindings.expects(:rd_kafka_subscribe).returns(20)

      assert_raises(Rdkafka::RdkafkaError) do
        consumer.subscribe(topic)
      end
    end

    it "raises an error when unsubscribing fails" do
      Rdkafka::Bindings.expects(:rd_kafka_unsubscribe).returns(20)

      assert_raises(Rdkafka::RdkafkaError) do
        consumer.unsubscribe
      end
    end

    it "raises an error when fetching the subscription fails" do
      Rdkafka::Bindings.expects(:rd_kafka_subscription).returns(20)

      assert_raises(Rdkafka::RdkafkaError) do
        consumer.subscription
      end
    end

    context "when using consumer without the poll set" do
      it "subscribe,s unsubscribe and return the subscription" do
        config = rdkafka_consumer_config
        config.consumer_poll_set = false
        @consumer = config.consumer

        assert_empty consumer.subscription

        consumer.subscribe(topic)

        refute_empty consumer.subscription
        expected_subscription = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
          list.add_topic(topic)
        end
        assert_equal expected_subscription, consumer.subscription

        consumer.unsubscribe

        assert_empty consumer.subscription
      end
    end
  end

  describe "#pause and #resume" do
    context "subscription" do
      before do
        @timeout = 2000
        consumer.subscribe(topic)
      end

      after { consumer.unsubscribe }

      it "pauses and then resume" do
        # 1. partitions are assigned
        wait_for_assignment(consumer)
        refute_empty consumer.assignment

        # 2. send a first message
        send_one_message

        # 3. ensure that message is successfully consumed
        records = consumer.poll(@timeout)
        refute_nil records
        consumer.commit

        # 4. send a second message
        send_one_message

        # 5. pause the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic(topic, 0..2)
        consumer.pause(tpl)

        # 6. ensure that messages are not available
        records = consumer.poll(@timeout)
        assert_nil records

        # 7. resume the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic(topic, 0..2)
        consumer.resume(tpl)

        # 8. ensure that message is successfully consumed
        records = consumer.poll(@timeout)
        refute_nil records
      end

      def send_one_message
        producer.produce(
          topic: topic,
          payload: "payload 1",
          key: "key 1"
        ).wait
      end
    end

    it "raises when not TopicPartitionList" do
      assert_raises(TypeError) { consumer.pause(true) }
      assert_raises(TypeError) { consumer.resume(true) }
    end

    it "raises an error when pausing fails" do
      list = Rdkafka::Consumer::TopicPartitionList.new.tap { |tpl| tpl.add_topic("topic", 0..1) }

      Rdkafka::Bindings.expects(:rd_kafka_pause_partitions).returns(20)
      err = assert_raises(Rdkafka::RdkafkaTopicPartitionListError) do
        consumer.pause(list)
      end
      assert_kind_of Rdkafka::RdkafkaTopicPartitionListError, err
      refute_nil err.topic_partition_list
    end

    it "raises an error when resume fails" do
      Rdkafka::Bindings.expects(:rd_kafka_resume_partitions).returns(20)
      assert_raises(Rdkafka::RdkafkaError) do
        consumer.resume(Rdkafka::Consumer::TopicPartitionList.new)
      end
    end
  end

  describe "#seek" do
    before do
      @seek_topic = "it-#{SecureRandom.uuid}"
      seek_admin = rdkafka_producer_config.admin
      seek_admin.create_topic(@seek_topic, 1, 1).wait
      wait_for_topic(seek_admin, @seek_topic)
      seek_admin.close
    end

    it "raises an error when seeking fails" do
      fake_msg = OpenStruct.new(topic: @seek_topic, partition: 0, offset: 0)

      Rdkafka::Bindings.expects(:rd_kafka_seek).returns(20)
      assert_raises(Rdkafka::RdkafkaError) do
        consumer.seek(fake_msg)
      end
    end

    context "subscription" do
      before do
        @timeout = 1000
        # Some specs here test the manual offset commit hence we want to ensure, that we have some
        # offsets in-memory that we can manually commit
        @consumer = rdkafka_consumer_config("auto.commit.interval.ms": 60_000).consumer

        consumer.subscribe(@seek_topic)

        # 1. partitions are assigned
        wait_for_assignment(consumer)
        refute_empty consumer.assignment

        # 2. eat unrelated messages
        while consumer.poll(@timeout) do; end
      end

      after { consumer.unsubscribe }

      def send_one_message(val)
        producer.produce(
          topic: @seek_topic,
          payload: "payload #{val}",
          key: "key 1",
          partition: 0
        ).wait
      end

      it "works when a partition is paused" do
        # 3. get reference message
        send_one_message(:a)
        message1 = consumer.poll(@timeout)
        assert_equal "payload a", message1&.payload

        # 4. pause the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic(@seek_topic, 1)
        consumer.pause(tpl)

        # 5. seek to previous message
        consumer.seek(message1)

        # 6. resume the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic(@seek_topic, 1)
        consumer.resume(tpl)

        # 7. ensure same message is read again
        message2 = consumer.poll(@timeout)

        # This is needed because `enable.auto.offset.store` is true but when running in CI that
        # is overloaded, offset store lags
        sleep(1)

        consumer.commit
        assert_equal message1.offset, message2.offset
        assert_equal message1.payload, message2.payload
      end

      it "allows skipping messages" do
        # 3. send messages
        send_one_message(:a)
        send_one_message(:b)
        send_one_message(:c)

        # 4. get reference message
        message = consumer.poll(@timeout)
        assert_equal "payload a", message&.payload

        # 5. seek over one message
        fake_msg = message.dup
        fake_msg.instance_variable_set(:@offset, fake_msg.offset + 2)
        consumer.seek(fake_msg)

        # 6. ensure that only one message is available
        records = consumer.poll(@timeout)
        assert_equal "payload c", records&.payload
        records = consumer.poll(@timeout)
        assert_nil records
      end
    end
  end

  describe "#seek_by" do
    before do
      @seek_by_topic = "it-#{SecureRandom.uuid}"
      @partition = 0
      @offset = 0
      @consumer = rdkafka_consumer_config("auto.commit.interval.ms": 60_000).consumer

      seek_admin = rdkafka_producer_config.admin
      seek_admin.create_topic(@seek_by_topic, 1, 1).wait
      wait_for_topic(seek_admin, @seek_by_topic)
      seek_admin.close
    end

    it "raises an error when seeking fails" do
      Rdkafka::Bindings.expects(:rd_kafka_seek).returns(20)
      assert_raises(Rdkafka::RdkafkaError) do
        consumer.seek_by(@seek_by_topic, @partition, @offset)
      end
    end

    context "subscription" do
      before do
        @timeout = 1000

        consumer.subscribe(@seek_by_topic)

        # 1. partitions are assigned
        wait_for_assignment(consumer)
        refute_empty consumer.assignment

        # 2. eat unrelated messages
        while consumer.poll(@timeout) do; end
      end

      after { consumer.unsubscribe }

      def send_one_message(val)
        producer.produce(
          topic: @seek_by_topic,
          payload: "payload #{val}",
          key: "key 1",
          partition: 0
        ).wait
      end

      it "works when a partition is paused" do
        # 3. get reference message
        send_one_message(:a)
        message1 = consumer.poll(@timeout)
        assert_equal "payload a", message1&.payload

        # 4. pause the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic(@seek_by_topic, 1)
        consumer.pause(tpl)

        # 5. seek by the previous message fields
        consumer.seek_by(message1.topic, message1.partition, message1.offset)

        # 6. resume the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic(@seek_by_topic, 1)
        consumer.resume(tpl)

        # 7. ensure same message is read again
        message2 = consumer.poll(@timeout)

        # This is needed because `enable.auto.offset.store` is true but when running in CI that
        # is overloaded, offset store lags
        sleep(2)

        consumer.commit
        assert_equal message1.offset, message2.offset
        assert_equal message1.payload, message2.payload
      end

      it "allows skipping messages" do
        # 3. send messages
        send_one_message(:a)
        send_one_message(:b)
        send_one_message(:c)

        # 4. get reference message
        message = consumer.poll(@timeout)
        assert_equal "payload a", message&.payload

        # 5. seek over one message
        consumer.seek_by(message.topic, message.partition, message.offset + 2)

        # 6. ensure that only one message is available
        records = consumer.poll(@timeout)
        assert_equal "payload c", records&.payload
        records = consumer.poll(@timeout)
        assert_nil records
      end
    end
  end

  describe "#assign and #assignment" do
    it "returns an empty assignment if nothing is assigned" do
      assert_empty consumer.assignment
    end

    it "onlies accept a topic partition list in assign" do
      assert_raises(TypeError) do
        consumer.assign("list")
      end
    end

    it "raises an error when assigning fails" do
      Rdkafka::Bindings.expects(:rd_kafka_assign).returns(20)
      assert_raises(Rdkafka::RdkafkaError) do
        consumer.assign(Rdkafka::Consumer::TopicPartitionList.new)
      end
    end

    it "assigns specific topic/partitions and return that assignment" do
      tpl = Rdkafka::Consumer::TopicPartitionList.new
      tpl.add_topic(topic, 0..2)
      consumer.assign(tpl)

      assignment = consumer.assignment
      refute_empty assignment
      assert_equal 3, assignment.to_h[topic].length
    end

    it "returns the assignment when subscribed" do
      # Make sure there's a message
      producer.produce(
        topic: topic,
        payload: "payload 1",
        key: "key 1",
        partition: 0
      ).wait

      # Subscribe and poll until partitions are assigned
      consumer.subscribe(topic)
      100.times do
        consumer.poll(100)
        break unless consumer.assignment.empty?
      end

      assignment = consumer.assignment
      refute_empty assignment
      assert_equal 3, assignment.to_h[topic].length
    end

    it "raises an error when getting assignment fails" do
      Rdkafka::Bindings.expects(:rd_kafka_assignment).returns(20)
      assert_raises(Rdkafka::RdkafkaError) do
        consumer.assignment
      end
    end
  end

  describe "#assignment_lost?" do
    it "does not return true as we do have an assignment" do
      consumer.subscribe(topic)
      Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic(topic)
      end

      assert_equal false, consumer.assignment_lost?
      consumer.unsubscribe
    end

    it "does not return true after voluntary unsubscribing" do
      consumer.subscribe(topic)
      Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic(topic)
      end

      consumer.unsubscribe
      assert_equal false, consumer.assignment_lost?
    end
  end

  describe "#close" do
    it "closes a consumer" do
      consumer.subscribe(topic)
      100.times do |i|
        producer.produce(
          topic: topic,
          payload: "payload #{i}",
          key: "key #{i}",
          partition: 0
        ).wait
      end
      consumer.close
      e = assert_raises(Rdkafka::ClosedConsumerError) do
        consumer.poll(100)
      end
      assert_match(/poll/, e.message)
    end

    context "when there are outgoing operations in other threads" do
      it "waits and not crash" do
        times = []
        # Force topic creation before spawning thread
        t = topic

        # Run a long running poll
        thread = Thread.new do
          times << Time.now
          consumer.subscribe(t)
          times << Time.now
          consumer.poll(1_000)
          times << Time.now
        end

        # Make sure it starts before we close
        sleep(0.1)
        consumer.close
        close_time = Time.now
        thread.join

        times.each { |time| assert_operator time, :<, close_time }
      end
    end
  end

  describe "#position, #commit, #committed and #store_offset" do
    # Make sure there are messages to work with
    before do
      @report = producer.produce(
        topic: topic,
        payload: "payload 1",
        key: "key 1",
        partition: 0
      ).wait
    end

    def fetched_message
      @fetched_message ||= wait_for_message(
        topic: topic,
        delivery_report: @report,
        consumer: consumer
      )
    end

    describe "#position" do
      it "onlies accept a topic partition list in position if not nil" do
        assert_raises(TypeError) do
          consumer.position("list")
        end
      end
    end

    describe "#committed" do
      it "onlies accept a topic partition list in commit if not nil" do
        assert_raises(TypeError) do
          consumer.commit("list")
        end
      end

      it "commits in sync mode" do
        consumer.commit(nil, true)
      end
    end

    context "with a committed consumer" do
      before do
        # Make sure there are some messages.
        handles = []
        10.times do
          (0..2).each do |i|
            handles << producer.produce(
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
        list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
          list.add_topic_and_partitions_with_offsets(topic, 0 => 1, 1 => 1, 2 => 1)
        end
        consumer.commit(list)
      end

      it "commits a specific topic partion list" do
        list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
          list.add_topic_and_partitions_with_offsets(topic, 0 => 1, 1 => 2, 2 => 3)
        end
        consumer.commit(list)

        partitions = consumer.committed(list).to_h[topic]
        assert_equal 1, partitions[0].offset
        assert_equal 2, partitions[1].offset
        assert_equal 3, partitions[2].offset
      end

      it "raises an error when committing fails" do
        Rdkafka::Bindings.expects(:rd_kafka_commit).returns(20)

        assert_raises(Rdkafka::RdkafkaError) do
          consumer.commit
        end
      end

      describe "#committed" do
        it "fetches the committed offsets for the current assignment" do
          partitions = consumer.committed.to_h[topic]
          refute_nil partitions
          assert_equal 1, partitions[0].offset
        end

        it "fetches the committed offsets for a specified topic partition list" do
          list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
            list.add_topic(topic, [0, 1, 2])
          end
          partitions = consumer.committed(list).to_h[topic]
          refute_nil partitions
          assert_equal 1, partitions[0].offset
          assert_equal 1, partitions[1].offset
          assert_equal 1, partitions[2].offset
        end

        it "raises an error when getting committed fails" do
          Rdkafka::Bindings.expects(:rd_kafka_committed).returns(20)
          list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
            list.add_topic(topic, [0, 1, 2])
          end
          assert_raises(Rdkafka::RdkafkaError) do
            consumer.committed(list)
          end
        end
      end

      describe "#store_offset" do
        before do
          @group_id = SecureRandom.uuid
          @base_config = {
            "group.id": @group_id,
            "enable.auto.offset.store": false,
            "enable.auto.commit": false
          }

          @store_offset_report = producer.produce(
            topic: topic,
            payload: "payload store_offset",
            key: "key store_offset",
            partition: 0
          ).wait

          @store_message = wait_for_message(
            topic: topic,
            delivery_report: @store_offset_report
          )

          @new_consumer = rdkafka_consumer_config(@base_config).consumer
          @new_consumer.subscribe(topic)
          wait_for_assignment(@new_consumer)
        end

        after do
          @new_consumer.close
        end

        it "stores the offset for a message" do
          @new_consumer.store_offset(@store_message)
          @new_consumer.commit

          list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
            list.add_topic(topic, [0, 1, 2])
          end
          partitions = @new_consumer.committed(list).to_h[topic]
          refute_nil partitions
          assert_equal @store_message.offset + 1, partitions[@store_message.partition].offset
        end

        it "raises an error with invalid input" do
          @store_message.stubs(:partition).returns(9999)
          assert_raises(Rdkafka::RdkafkaError) do
            @new_consumer.store_offset(@store_message)
          end
        end

        describe "#position" do
          it "fetches the positions for the current assignment" do
            # consumer must poll the message directly (not via a separate consumer)
            # for position to reflect the fetch offset
            @consumer = rdkafka_consumer_config("enable.auto.offset.store": false).consumer
            report = producer.produce(
              topic: topic,
              payload: "payload position",
              key: "key position",
              partition: 0
            ).wait
            polled_message = wait_for_message(
              topic: topic,
              delivery_report: report,
              consumer: consumer
            )

            consumer.store_offset(polled_message)

            partitions = consumer.position.to_h[topic]
            refute_nil partitions
            assert_equal polled_message.offset + 1, partitions[0].offset
          end

          it "fetches the positions for a specified assignment" do
            @consumer = rdkafka_consumer_config("enable.auto.offset.store": false).consumer
            report = producer.produce(
              topic: topic,
              payload: "payload position",
              key: "key position",
              partition: 0
            ).wait
            polled_message = wait_for_message(
              topic: topic,
              delivery_report: report,
              consumer: consumer
            )

            consumer.store_offset(polled_message)

            list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
              list.add_topic_and_partitions_with_offsets(topic, 0 => nil, 1 => nil, 2 => nil)
            end
            partitions = consumer.position(list).to_h[topic]
            refute_nil partitions
            assert_equal polled_message.offset + 1, partitions[0].offset
          end

          it "raises an error when getting the position fails" do
            Rdkafka::Bindings.expects(:rd_kafka_position).returns(20)

            assert_raises(Rdkafka::RdkafkaError) do
              consumer.position
            end
          end
        end

        context "when trying to use with enable.auto.offset.store set to true" do
          it "expect to raise invalid configuration error" do
            auto_store_consumer = rdkafka_consumer_config("enable.auto.offset.store": true).consumer
            begin
              e = assert_raises(Rdkafka::RdkafkaError) do
                auto_store_consumer.store_offset(@store_message)
              end
              assert_match(/invalid_arg/, e.message)
            ensure
              auto_store_consumer.close
            end
          end
        end
      end
    end
  end

  describe "#query_watermark_offsets" do
    it "returns the watermark offsets" do
      # Make sure there's a message
      producer.produce(
        topic: topic,
        payload: "payload 1",
        key: "key 1",
        partition: 0
      ).wait

      low, high = consumer.query_watermark_offsets(topic, 0, 5000)
      assert_equal 0, low
      assert_operator high, :>, 0
    end

    it "raises an error when querying offsets fails" do
      Rdkafka::Bindings.expects(:rd_kafka_query_watermark_offsets).returns(20)
      assert_raises(Rdkafka::RdkafkaError) do
        consumer.query_watermark_offsets(topic, 0, 5000)
      end
    end
  end

  describe "#lag" do
    it "calculates the consumer lag" do
      @consumer = rdkafka_consumer_config("enable.partition.eof": true).consumer

      # Make sure there's a message in every partition and
      # wait for the message to make sure everything is committed.
      (0..2).each do |i|
        producer.produce(
          topic: topic,
          key: "key lag #{i}",
          partition: i
        ).wait
      end

      # Consume to the end
      consumer.subscribe(topic)
      eof_count = 0
      loop do
        consumer.poll(100)
      rescue Rdkafka::RdkafkaError => error
        if error.is_partition_eof?
          eof_count += 1
        end
        break if eof_count == 3
      end

      # Commit
      consumer.commit

      # Create list to fetch lag for.
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
        producer.produce(
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
    end

    it "returns nil if there are no messages on the topic" do
      @consumer = rdkafka_consumer_config("enable.partition.eof": true).consumer

      # Subscribe first to establish a group coordinator, otherwise
      # committed() can fail with not_coordinator in random test order
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
    end
  end

  describe "#cluster_id" do
    it "returns the current ClusterId" do
      consumer.subscribe(topic)
      wait_for_assignment(consumer)
      refute_empty consumer.cluster_id
    end
  end

  describe "#member_id" do
    it "returns the current MemberId" do
      consumer.subscribe(topic)
      wait_for_assignment(consumer)
      assert_match(/\Ardkafka-/, consumer.member_id)
    end
  end

  describe "#poll" do
    it "returns nil if there is no subscription" do
      assert_nil consumer.poll(1000)
    end

    it "returns nil if there are no messages" do
      consumer.subscribe(topic)
      assert_nil consumer.poll(1000)
    end

    it "returns a message if there is one" do
      poll_topic = "it-#{SecureRandom.uuid}"

      producer.produce(
        topic: poll_topic,
        payload: "payload 1",
        key: "key 1"
      ).wait
      consumer.subscribe(poll_topic)
      message = consumer.each { |m| break m }

      assert_kind_of Rdkafka::Consumer::Message, message
      assert_equal "payload 1", message.payload
      assert_equal "key 1", message.key
    end

    it "raises an error when polling fails" do
      message = Rdkafka::Bindings::Message.new.tap do |message|
        message[:err] = 20
      end
      message_pointer = message.to_ptr
      Rdkafka::Bindings.expects(:rd_kafka_consumer_poll).returns(message_pointer)
      Rdkafka::Bindings.expects(:rd_kafka_message_destroy).with(message_pointer)
      assert_raises(Rdkafka::RdkafkaError) do
        consumer.poll(100)
      end
    end
  end

  describe "#poll_nb" do
    it "returns nil if there is no subscription" do
      assert_nil consumer.poll_nb
    end

    it "returns nil if there are no messages" do
      consumer.subscribe(topic)
      assert_nil consumer.poll_nb
    end

    it "accepts a timeout parameter" do
      consumer.subscribe(topic)
      assert_nil consumer.poll_nb(0)
      assert_nil consumer.poll_nb(100)
    end

    it "returns a message if there is one" do
      poll_nb_topic = "it-#{SecureRandom.uuid}"

      producer.produce(
        topic: poll_nb_topic,
        payload: "payload poll_nb",
        key: "key poll_nb"
      ).wait

      consumer.subscribe(poll_nb_topic)
      wait_for_assignment(consumer)

      # Give time for message to arrive
      sleep 1

      message = nil
      10.times do
        message = consumer.poll_nb(100)
        break if message
        sleep 0.1
      end

      assert_kind_of Rdkafka::Consumer::Message, message
      assert_equal "payload poll_nb", message.payload
      assert_equal "key poll_nb", message.key
    end

    it "raises an error when polling fails" do
      message = Rdkafka::Bindings::Message.new.tap do |message|
        message[:err] = 20
      end
      message_pointer = message.to_ptr
      Rdkafka::Bindings.expects(:rd_kafka_consumer_poll_nb).returns(message_pointer)
      Rdkafka::Bindings.expects(:rd_kafka_message_destroy).with(message_pointer)
      assert_raises(Rdkafka::RdkafkaError) do
        consumer.poll_nb
      end
    end

    context "when consumer is closed" do
      before { consumer.close }

      it "raises ClosedConsumerError" do
        e = assert_raises(Rdkafka::ClosedConsumerError) do
          consumer.poll_nb
        end
        assert_match(/poll_nb/, e.message)
      end
    end
  end

  describe "#poll with headers" do
    it "returns message with headers using string keys (when produced with symbol keys)" do
      report = producer.produce(
        topic: topic,
        key: "key headers",
        headers: { foo: "bar" }
      ).wait

      message = wait_for_message(topic: topic, consumer: consumer, delivery_report: report)
      refute_nil message
      assert_equal "key headers", message.key
      assert_includes message.headers, "foo"
      assert_equal "bar", message.headers["foo"]
    end

    it "returns message with headers using string keys (when produced with string keys)" do
      report = producer.produce(
        topic: topic,
        key: "key headers",
        headers: { "foo" => "bar" }
      ).wait

      message = wait_for_message(topic: topic, consumer: consumer, delivery_report: report)
      refute_nil message
      assert_equal "key headers", message.key
      assert_includes message.headers, "foo"
      assert_equal "bar", message.headers["foo"]
    end

    it "returns message with no headers" do
      report = producer.produce(
        topic: topic,
        key: "key no headers",
        headers: nil
      ).wait

      message = wait_for_message(topic: topic, consumer: consumer, delivery_report: report)
      refute_nil message
      assert_equal "key no headers", message.key
      assert_empty message.headers
    end

    it "raises an error when message headers aren't readable" do
      Rdkafka::Bindings.expects(:rd_kafka_message_headers).with(anything, anything).returns(1)

      report = producer.produce(
        topic: topic,
        key: "key err headers",
        headers: nil
      ).wait

      err = assert_raises(Rdkafka::RdkafkaError) do
        wait_for_message(topic: topic, consumer: consumer, delivery_report: report)
      end
      assert_kind_of Rdkafka::RdkafkaError, err
      assert_match(/\AError reading message headers/, err.message)
    end

    it "raises an error when the first message header aren't readable" do
      Rdkafka::Bindings.expects(:rd_kafka_header_get_all).with(anything, anything, anything, anything, anything).returns(1)

      report = producer.produce(
        topic: topic,
        key: "key err headers",
        headers: { foo: "bar" }
      ).wait

      err = assert_raises(Rdkafka::RdkafkaError) do
        wait_for_message(topic: topic, consumer: consumer, delivery_report: report)
      end
      assert_kind_of Rdkafka::RdkafkaError, err
      assert_match(/\AError reading a message header at index 0/, err.message)
    end
  end

  describe "#each" do
    it "yields messages" do
      handles = []
      10.times do
        handles << producer.produce(
          topic: topic,
          payload: "payload 1",
          key: "key 1",
          partition: 0
        )
      end
      handles.each(&:wait)

      consumer.subscribe(topic)
      # Check the first 10 messages. Then close the consumer, which
      # should break the each loop.
      consumer.each_with_index do |message, i|
        assert_kind_of Rdkafka::Consumer::Message, message
        break if i == 9
      end
      consumer.close
    end
  end

  describe "#each_batch" do
    it "expect to raise an error" do
      assert_raises(NotImplementedError) do
        consumer.each_batch {}
      end
    end
  end

  describe "#offsets_for_times" do
    it "raises when not TopicPartitionList" do
      assert_raises(TypeError) { consumer.offsets_for_times([]) }
    end

    it "raises an error when offsets_for_times fails" do
      tpl = Rdkafka::Consumer::TopicPartitionList.new

      Rdkafka::Bindings.expects(:rd_kafka_offsets_for_times).returns(7)

      assert_raises(Rdkafka::RdkafkaError) { consumer.offsets_for_times(tpl) }
    end

    context "when subscribed" do
      before do
        @timeout = 1000
        consumer.subscribe(topic)

        # 1. partitions are assigned
        wait_for_assignment(consumer)
        refute_empty consumer.assignment

        # 2. eat unrelated messages
        while consumer.poll(@timeout) do; end
      end

      after { consumer.unsubscribe }

      def send_one_message(val)
        producer.produce(
          topic: topic,
          payload: "payload #{val}",
          key: "key 0",
          partition: 0
        ).wait
      end

      it "returns a TopicParticionList with updated offsets" do
        send_one_message("a")
        send_one_message("b")
        send_one_message("c")

        consumer.poll(@timeout)
        message = consumer.poll(@timeout)
        consumer.poll(@timeout)

        tpl = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
          list.add_topic_and_partitions_with_offsets(
            topic,
            [
              [0, message.timestamp]
            ]
          )
        end

        tpl_response = consumer.offsets_for_times(tpl)

        assert_equal message.offset, tpl_response.to_h[topic][0].offset
      end
    end
  end

  # Only relevant in case of a consumer with separate queues
  describe "#events_poll" do
    it "expect to run events_poll, operate and propagate stats on events_poll and not poll" do
      stats = []
      Rdkafka::Config.statistics_callback = ->(published) { stats << published }

      config = rdkafka_consumer_config("statistics.interval.ms": 500)
      config.consumer_poll_set = false
      @consumer = config.consumer

      consumer.subscribe(topic)
      consumer.poll(1_000)
      assert_empty stats
      consumer.events_poll(-1)
      refute_empty stats
    ensure
      Rdkafka::Config.statistics_callback = nil
    end
  end

  # Only relevant in case of a consumer with separate queues
  describe "#events_poll_nb" do
    before do
      @stats = []
      Rdkafka::Config.statistics_callback = ->(published) { @stats << published }

      config = rdkafka_consumer_config("statistics.interval.ms": 500)
      config.consumer_poll_set = false
      @consumer = config.consumer
    end

    after { Rdkafka::Config.statistics_callback = nil }

    it "returns the number of events processed" do
      consumer.subscribe(topic)
      result = consumer.events_poll_nb
      assert_kind_of Integer, result
      assert_operator result, :>=, 0
    end

    it "accepts a timeout parameter" do
      consumer.subscribe(topic)
      assert_operator consumer.events_poll_nb(0), :>=, 0
      assert_operator consumer.events_poll_nb(100), :>=, 0
    end

    it "processes events without releasing GVL" do
      consumer.subscribe(topic)
      consumer.poll(1_000)
      assert_empty @stats

      # Wait for statistics to be ready
      sleep 0.6

      # Non-blocking poll should also process stats events
      consumer.events_poll_nb(100)
      refute_empty @stats
    end
  end

  describe "#consumer_group_metadata_pointer" do
    it "expect to return a pointer" do
      pointer = consumer.consumer_group_metadata_pointer
      begin
        assert_kind_of FFI::Pointer, pointer
      ensure
        Rdkafka::Bindings.rd_kafka_consumer_group_metadata_destroy(pointer)
      end
    end
  end

  describe "a rebalance listener" do
    context "with a working listener" do
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
        @consumer = config.consumer

        notify_listener(listener, topic: topic)

        assert_equal [
          [:assign, topic, 0, 1, 2],
          [:revoke, topic, 0, 1, 2]
        ], listener.queue
      end
    end

    context "with a broken listener" do
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
        @consumer = config.consumer

        notify_listener(listener, topic: topic)

        assert_equal [:assigned, :revoked], listener.queue
      end
    end
  end

  context "methods that should not be called after a consumer has been closed" do
    before do
      consumer.close
    end

    # Affected methods and a non-invalid set of parameters for the method
    {
      subscribe: [nil],
      unsubscribe: nil,
      pause: [nil],
      resume: [nil],
      subscription: nil,
      assign: [nil],
      assignment: nil,
      committed: [],
      query_watermark_offsets: [nil, nil],
      assignment_lost?: [],
      poll_nb: []
    }.each do |method, args|
      it "raises an exception if #{method} is called" do
        e = assert_raises(Rdkafka::ClosedConsumerError) do
          if args.nil?
            consumer.public_send(method)
          else
            consumer.public_send(method, *args)
          end
        end
        assert_match(/#{method}/, e.message)
      end
    end
  end

  it "provides a finalizer that closes the native kafka client" do
    assert_equal false, consumer.closed?

    consumer.finalizer.call("some-ignored-object-id")

    assert_equal true, consumer.closed?
  end

  context "when the rebalance protocol is cooperative" do
    it "is able to assign and unassign partitions using the cooperative partition assignment APIs" do
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
      @consumer = config.consumer

      notify_listener(listener, topic: topic) do
        handles = []
        10.times do
          handles << producer.produce(
            topic: topic,
            payload: "payload 1",
            key: "key 1",
            partition: 0
          )
        end
        handles.each(&:wait)

        consumer.subscribe(topic)
        # Check the first 10 messages. Then close the consumer, which
        # should break the each loop.
        consumer.each_with_index do |message, i|
          assert_kind_of Rdkafka::Consumer::Message, message
          break if i == 9
        end
      end

      assert_equal [
        [:assign, topic, 0, 1, 2],
        [:revoke, topic, 0, 1, 2]
      ], listener.queue
    end
  end

  describe "#oauthbearer_set_token" do
    context "when sasl not configured" do
      it "returns RD_KAFKA_RESP_ERR__STATE" do
        response = consumer.oauthbearer_set_token(
          token: "foo",
          lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
          principal_name: "kafka-cluster"
        )
        assert_equal Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE, response
      end
    end

    context "when sasl configured" do
      before do
        @consumer_sasl = rdkafka_producer_config(
          "security.protocol": "sasl_ssl",
          "sasl.mechanisms": "OAUTHBEARER"
        ).consumer
      end

      after do
        @consumer_sasl.close
      end

      context "without extensions" do
        it "succeeds" do
          response = @consumer_sasl.oauthbearer_set_token(
            token: "foo",
            lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
            principal_name: "kafka-cluster"
          )
          assert_equal 0, response
        end
      end

      context "with extensions" do
        it "succeeds" do
          response = @consumer_sasl.oauthbearer_set_token(
            token: "foo",
            lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
            principal_name: "kafka-cluster",
            extensions: {
              "foo" => "bar"
            }
          )
          assert_equal 0, response
        end
      end
    end
  end

  describe "when reaching eof on a topic and eof reporting enabled" do
    it "returns proper details" do
      @consumer = rdkafka_consumer_config("enable.partition.eof": true).consumer

      (0..2).each do |i|
        producer.produce(
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
    end
  end

  describe "long running consumption" do
    it "consumes messages continuously for 60 seconds" do
      @consumer = rdkafka_consumer_config.consumer
      @producer = rdkafka_producer_config.producer

      consumer.subscribe(topic)
      wait_for_assignment(consumer)

      messages_consumed = 0
      start_time = Time.now

      # Producer thread - sends message every second
      producer_thread = Thread.new do
        counter = 0
        while Time.now - start_time < 60
          producer.produce(
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
        message = consumer.poll(1000)
        if message
          assert_kind_of Rdkafka::Consumer::Message, message
          assert_equal topic, message.topic
          messages_consumed += 1
          consumer.commit if messages_consumed % 10 == 0
        end
      end

      producer_thread.join

      assert_operator messages_consumed, :>, 50 # Should consume most messages
    end
  end

  describe "#events_poll_nb_each" do
    it "does not raise when queue is empty" do
      consumer.events_poll_nb_each { |_| }
    end

    it "yields the count after each poll" do
      counts = []
      # Stub to return events, then zero
      call_count = 0
      Rdkafka::Bindings.stubs(:rd_kafka_poll_nb).with do
        call_count += 1
        true
      end.returns(1, 1, 0)

      consumer.events_poll_nb_each { |count| counts << count }

      assert_equal [1, 1], counts
    end

    it "stops when block returns :stop" do
      iterations = 0
      # Stub to always return events
      Rdkafka::Bindings.stubs(:rd_kafka_poll_nb).returns(1)

      consumer.events_poll_nb_each do |_count|
        iterations += 1
        :stop if iterations >= 3
      end

      assert_equal 3, iterations
    end

    context "when consumer is closed" do
      before { consumer.close }

      it "raises ClosedConsumerError" do
        e = assert_raises(Rdkafka::ClosedConsumerError) do
          consumer.events_poll_nb_each { |_| }
        end
        assert_match(/events_poll_nb_each/, e.message)
      end
    end
  end

  describe "#poll_nb_each" do
    it "does not raise when queue is empty" do
      consumer.subscribe(topic)
      # Give it a moment to subscribe
      sleep 0.5

      messages = []
      consumer.poll_nb_each { |msg| messages << msg }
      assert_kind_of Array, messages
    end

    it "yields messages and respects :stop" do
      consumer.subscribe(topic)

      # Produce some messages
      5.times { |i| producer.produce(topic: topic, payload: "poll_nb_each test #{i}") }
      producer.flush

      # Consume messages with blocking poll to ensure they are fetched into the buffer
      deadline = Time.now + 30
      consumed = 0
      while Time.now < deadline && consumed < 2
        msg = consumer.poll(100)
        consumed += 1 if msg
      end

      # Produce more messages to ensure poll_nb_each has something to yield
      3.times { |i| producer.produce(topic: topic, payload: "poll_nb_each extra #{i}") }
      producer.flush
      sleep 1

      # Now test that :stop works
      messages = []
      consumer.poll_nb_each do |message|
        messages << message
        :stop if messages.size >= 1
      end

      assert_equal 1, messages.size
    end

    it "properly cleans up message pointers" do
      consumer.subscribe(topic)

      producer.produce(topic: topic, payload: "cleanup test")
      producer.flush
      sleep 2

      # This should not leak memory - message_destroy is called in ensure
      consumer.poll_nb_each { |_| }
    end

    context "when consumer is closed" do
      before { consumer.close }

      it "raises ClosedConsumerError" do
        e = assert_raises(Rdkafka::ClosedConsumerError) do
          consumer.poll_nb_each { |_| }
        end
        assert_match(/poll_nb_each/, e.message)
      end
    end
  end

  describe "file descriptor access for fiber scheduler integration" do
    it "enables IO events on consumer queue" do
      consumer.subscribe(topic)

      signal_r, signal_w = IO.pipe
      consumer.enable_queue_io_events(signal_w.fileno)
      signal_r.close
      signal_w.close
    end

    it "enables IO events on background queue" do
      consumer.subscribe(topic)

      signal_r, signal_w = IO.pipe
      consumer.enable_background_queue_io_events(signal_w.fileno)
      signal_r.close
      signal_w.close
    end

    it "enables FD with payload option" do
      consumer.subscribe(topic)

      signal_r, signal_w = IO.pipe
      custom_payload = "hello"
      consumer.enable_queue_io_events(signal_w.fileno, custom_payload)
      signal_r.close
      signal_w.close
    end

    it "supports normal polling with IO events enabled" do
      consumer.subscribe(topic)

      # Setup IO event signaling
      signal_r, signal_w = IO.pipe
      consumer.enable_queue_io_events(signal_w.fileno)

      # Produce some messages
      producer.produce(topic: topic, payload: "test message 1")
      producer.produce(topic: topic, payload: "test message 2")
      producer.flush

      # Give consumer time to rebalance and fetch
      sleep 2

      # Try to poll messages directly
      messages = []
      10.times do
        msg = consumer.poll(100)
        messages << msg if msg
      end

      # We may or may not get messages depending on rebalancing, but should not error
      assert_kind_of Array, messages
      signal_r.close
      signal_w.close
    end

    context "when consumer is closed" do
      before { consumer.close }

      it "raises ClosedInnerError when enabling queue_io_events" do
        signal_r, signal_w = IO.pipe
        assert_raises(Rdkafka::ClosedInnerError) do
          consumer.enable_queue_io_events(signal_w.fileno)
        end
        signal_r.close
        signal_w.close
      end

      it "raises ClosedInnerError when enabling background_queue_io_events" do
        signal_r, signal_w = IO.pipe
        assert_raises(Rdkafka::ClosedInnerError) do
          consumer.enable_background_queue_io_events(signal_w.fileno)
        end
        signal_r.close
        signal_w.close
      end
    end
  end
end
