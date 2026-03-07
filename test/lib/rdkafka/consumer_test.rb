# frozen_string_literal: true

require "test_helper"
require "ostruct"
require "securerandom"

class ConsumerTest < Minitest::Test
  def setup
    super
    @consumer = rdkafka_consumer_config.consumer
    @producer = rdkafka_producer_config.producer
  end

  def teardown
    @consumer&.close
    @producer&.close
    super
  end

  # -- #name --

  def test_name_includes_consumer_prefix
    assert_includes @consumer.name, "rdkafka#consumer-"
  end

  # -- consumer without auto-start --

  def test_consumer_without_auto_start_can_start_later_and_close
    consumer = rdkafka_consumer_config.consumer(native_kafka_auto_start: false)
    consumer.start
    consumer.close
  end

  def test_consumer_without_auto_start_can_close_without_starting
    consumer = rdkafka_consumer_config.consumer(native_kafka_auto_start: false)
    consumer.close
  end

  # -- #subscribe, #unsubscribe and #subscription --

  def test_subscribe_unsubscribe_and_return_the_subscription
    assert_empty @consumer.subscription

    @consumer.subscribe(TestTopics.consume_test_topic)

    refute_empty @consumer.subscription
    expected_subscription = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
      list.add_topic(TestTopics.consume_test_topic)
    end

    assert_equal expected_subscription, @consumer.subscription

    @consumer.unsubscribe

    assert_empty @consumer.subscription
  end

  def test_subscribe_raises_an_error_when_subscribing_fails
    Rdkafka::Bindings.stub(:rd_kafka_subscribe, 20) do
      assert_raises(Rdkafka::RdkafkaError) do
        @consumer.subscribe(TestTopics.consume_test_topic)
      end
    end
  end

  def test_subscribe_raises_an_error_when_unsubscribing_fails
    Rdkafka::Bindings.stub(:rd_kafka_unsubscribe, 20) do
      assert_raises(Rdkafka::RdkafkaError) do
        @consumer.unsubscribe
      end
    end
  end

  def test_subscribe_raises_an_error_when_fetching_the_subscription_fails
    Rdkafka::Bindings.stub(:rd_kafka_subscription, 20) do
      assert_raises(Rdkafka::RdkafkaError) do
        @consumer.subscription
      end
    end
  end

  def test_subscribe_without_poll_set
    config = rdkafka_consumer_config
    config.consumer_poll_set = false
    consumer = config.consumer

    assert_empty consumer.subscription

    consumer.subscribe(TestTopics.consume_test_topic)

    refute_empty consumer.subscription
    expected_subscription = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
      list.add_topic(TestTopics.consume_test_topic)
    end

    assert_equal expected_subscription, consumer.subscription

    consumer.unsubscribe

    assert_empty consumer.subscription
  ensure
    consumer&.close
  end

  # -- #pause and #resume --

  def test_pause_and_resume_subscription
    timeout = 2000
    @consumer.subscribe(TestTopics.consume_test_topic)

    # 1. partitions are assigned
    wait_for_assignment(@consumer)

    refute_empty @consumer.assignment

    # 2. send a first message
    @producer.produce(
      topic: TestTopics.consume_test_topic,
      payload: "payload 1",
      key: "key 1"
    ).wait

    # 3. ensure that message is successfully consumed
    records = @consumer.poll(timeout)

    refute_nil records
    @consumer.commit

    # 4. send a second message
    @producer.produce(
      topic: TestTopics.consume_test_topic,
      payload: "payload 1",
      key: "key 1"
    ).wait

    # 5. pause the subscription
    tpl = Rdkafka::Consumer::TopicPartitionList.new
    tpl.add_topic(TestTopics.consume_test_topic, 0..2)
    @consumer.pause(tpl)

    # 6. ensure that messages are not available
    records = @consumer.poll(timeout)

    assert_nil records

    # 7. resume the subscription
    tpl = Rdkafka::Consumer::TopicPartitionList.new
    tpl.add_topic(TestTopics.consume_test_topic, 0..2)
    @consumer.resume(tpl)

    # 8. ensure that message is successfully consumed
    records = @consumer.poll(timeout)

    refute_nil records

    @consumer.unsubscribe
  end

  def test_pause_and_resume_raises_when_not_topic_partition_list
    assert_raises(TypeError) { @consumer.pause(true) }
    assert_raises(TypeError) { @consumer.resume(true) }
  end

  def test_pause_raises_an_error_when_pausing_fails
    list = Rdkafka::Consumer::TopicPartitionList.new.tap { |tpl| tpl.add_topic("topic", 0..1) }

    Rdkafka::Bindings.stub(:rd_kafka_pause_partitions, 20) do
      err = assert_raises(Rdkafka::RdkafkaTopicPartitionListError) do
        @consumer.pause(list)
      end
      refute_nil err.topic_partition_list
    end
  end

  def test_resume_raises_an_error_when_resume_fails
    Rdkafka::Bindings.stub(:rd_kafka_resume_partitions, 20) do
      assert_raises(Rdkafka::RdkafkaError) do
        @consumer.resume(Rdkafka::Consumer::TopicPartitionList.new)
      end
    end
  end

  # -- #seek --

  def test_seek_raises_an_error_when_seeking_fails
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

  def test_seek_works_when_a_partition_is_paused
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

  def test_seek_allows_skipping_messages
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

  # -- #seek_by --

  def test_seek_by_raises_an_error_when_seeking_fails
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

  def test_seek_by_works_when_a_partition_is_paused
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

  def test_seek_by_allows_skipping_messages
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

  # -- #assign and #assignment --

  def test_assign_returns_an_empty_assignment_if_nothing_is_assigned
    assert_empty @consumer.assignment
  end

  def test_assign_only_accepts_a_topic_partition_list
    assert_raises(TypeError) do
      @consumer.assign("list")
    end
  end

  def test_assign_raises_an_error_when_assigning_fails
    Rdkafka::Bindings.stub(:rd_kafka_assign, 20) do
      assert_raises(Rdkafka::RdkafkaError) do
        @consumer.assign(Rdkafka::Consumer::TopicPartitionList.new)
      end
    end
  end

  def test_assign_specific_topic_partitions_and_return_that_assignment
    tpl = Rdkafka::Consumer::TopicPartitionList.new
    tpl.add_topic(TestTopics.consume_test_topic, 0..2)
    @consumer.assign(tpl)

    assignment = @consumer.assignment

    refute_empty assignment
    assert_equal 3, assignment.to_h[TestTopics.consume_test_topic].length
  end

  def test_assign_returns_the_assignment_when_subscribed
    # Make sure there's a message
    @producer.produce(
      topic: TestTopics.consume_test_topic,
      payload: "payload 1",
      key: "key 1",
      partition: 0
    ).wait

    # Subscribe and poll until partitions are assigned
    @consumer.subscribe(TestTopics.consume_test_topic)
    100.times do
      @consumer.poll(100)
      break unless @consumer.assignment.empty?
    end

    assignment = @consumer.assignment

    refute_empty assignment
    assert_equal 3, assignment.to_h[TestTopics.consume_test_topic].length
  end

  def test_assign_raises_an_error_when_getting_assignment_fails
    Rdkafka::Bindings.stub(:rd_kafka_assignment, 20) do
      assert_raises(Rdkafka::RdkafkaError) do
        @consumer.assignment
      end
    end
  end

  # -- #assignment_lost? --

  def test_assignment_lost_does_not_return_true_when_we_have_an_assignment
    @consumer.subscribe(TestTopics.consume_test_topic)
    Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
      list.add_topic(TestTopics.consume_test_topic)
    end

    refute_predicate @consumer, :assignment_lost?
    @consumer.unsubscribe
  end

  def test_assignment_lost_does_not_return_true_after_voluntary_unsubscribing
    @consumer.subscribe(TestTopics.consume_test_topic)
    Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
      list.add_topic(TestTopics.consume_test_topic)
    end

    @consumer.unsubscribe

    refute_predicate @consumer, :assignment_lost?
  end

  # -- #close --

  def test_close_closes_a_consumer
    @consumer.subscribe(TestTopics.consume_test_topic)
    100.times do |i|
      @producer.produce(
        topic: TestTopics.consume_test_topic,
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

  def test_close_waits_for_outgoing_operations_in_other_threads
    times = []

    # Run a long running poll
    thread = Thread.new do
      times << Time.now
      @consumer.subscribe(TestTopics.empty_test_topic)
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

  # -- #position, #commit, #committed and #store_offset --

  def test_position_only_accepts_a_topic_partition_list_if_not_nil
    assert_raises(TypeError) do
      @consumer.position("list")
    end
  end

  def test_committed_only_accepts_a_topic_partition_list_if_not_nil
    assert_raises(TypeError) do
      @consumer.commit("list")
    end
  end

  def test_committed_commits_in_sync_mode
    @consumer.commit(nil, true)
  end

  def test_committed_commits_a_specific_topic_partition_list
    # Make sure there are some messages
    handles = []
    10.times do
      (0..2).each do |i|
        handles << @producer.produce(
          topic: TestTopics.consume_test_topic,
          payload: "payload 1",
          key: "key 1",
          partition: i
        )
      end
    end
    handles.each(&:wait)

    @consumer.subscribe(TestTopics.consume_test_topic)
    wait_for_assignment(@consumer)
    list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
      l.add_topic_and_partitions_with_offsets(TestTopics.consume_test_topic, 0 => 1, 1 => 1, 2 => 1)
    end
    @consumer.commit(list)

    list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
      l.add_topic_and_partitions_with_offsets(TestTopics.consume_test_topic, 0 => 1, 1 => 2, 2 => 3)
    end
    @consumer.commit(list)

    partitions = @consumer.committed(list).to_h[TestTopics.consume_test_topic]

    assert_equal 1, partitions[0].offset
    assert_equal 2, partitions[1].offset
    assert_equal 3, partitions[2].offset
  end

  def test_committed_raises_an_error_when_committing_fails
    # Need to subscribe and set up committed state first
    handles = []
    10.times do
      (0..2).each do |i|
        handles << @producer.produce(
          topic: TestTopics.consume_test_topic,
          payload: "payload 1",
          key: "key 1",
          partition: i
        )
      end
    end
    handles.each(&:wait)

    @consumer.subscribe(TestTopics.consume_test_topic)
    wait_for_assignment(@consumer)
    list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
      l.add_topic_and_partitions_with_offsets(TestTopics.consume_test_topic, 0 => 1, 1 => 1, 2 => 1)
    end
    @consumer.commit(list)

    Rdkafka::Bindings.stub(:rd_kafka_commit, 20) do
      assert_raises(Rdkafka::RdkafkaError) do
        @consumer.commit
      end
    end
  end

  def test_committed_fetches_the_committed_offsets_for_the_current_assignment
    # Setup committed state
    handles = []
    10.times do
      (0..2).each do |i|
        handles << @producer.produce(
          topic: TestTopics.consume_test_topic,
          payload: "payload 1",
          key: "key 1",
          partition: i
        )
      end
    end
    handles.each(&:wait)

    @consumer.subscribe(TestTopics.consume_test_topic)
    wait_for_assignment(@consumer)
    list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
      l.add_topic_and_partitions_with_offsets(TestTopics.consume_test_topic, 0 => 1, 1 => 1, 2 => 1)
    end
    @consumer.commit(list)

    partitions = @consumer.committed.to_h[TestTopics.consume_test_topic]

    refute_nil partitions
    assert_equal 1, partitions[0].offset
  end

  def test_committed_fetches_the_committed_offsets_for_a_specified_topic_partition_list
    # Setup committed state
    handles = []
    10.times do
      (0..2).each do |i|
        handles << @producer.produce(
          topic: TestTopics.consume_test_topic,
          payload: "payload 1",
          key: "key 1",
          partition: i
        )
      end
    end
    handles.each(&:wait)

    @consumer.subscribe(TestTopics.consume_test_topic)
    wait_for_assignment(@consumer)
    list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
      l.add_topic_and_partitions_with_offsets(TestTopics.consume_test_topic, 0 => 1, 1 => 1, 2 => 1)
    end
    @consumer.commit(list)

    list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
      l.add_topic(TestTopics.consume_test_topic, [0, 1, 2])
    end
    partitions = @consumer.committed(list).to_h[TestTopics.consume_test_topic]

    refute_nil partitions
    assert_equal 1, partitions[0].offset
    assert_equal 1, partitions[1].offset
    assert_equal 1, partitions[2].offset
  end

  def test_committed_raises_an_error_when_getting_committed_fails
    Rdkafka::Bindings.stub(:rd_kafka_committed, 20) do
      list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic(TestTopics.consume_test_topic, [0, 1, 2])
      end
      assert_raises(Rdkafka::RdkafkaError) do
        @consumer.committed(list)
      end
    end
  end

  # -- #store_offset --

  def test_store_offset_stores_the_offset_for_a_message
    report = @producer.produce(
      topic: TestTopics.consume_test_topic,
      payload: "payload 1",
      key: "key 1",
      partition: 0
    ).wait

    message = wait_for_message(
      topic: TestTopics.consume_test_topic,
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
    new_consumer.subscribe(TestTopics.consume_test_topic)
    wait_for_assignment(new_consumer)

    new_consumer.store_offset(message)
    new_consumer.commit

    list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
      l.add_topic(TestTopics.consume_test_topic, [0, 1, 2])
    end
    partitions = new_consumer.committed(list).to_h[TestTopics.consume_test_topic]

    refute_nil partitions
    assert_equal(message.offset + 1, partitions[message.partition].offset)
  ensure
    new_consumer&.close
  end

  def test_store_offset_raises_an_error_with_invalid_input
    report = @producer.produce(
      topic: TestTopics.consume_test_topic,
      payload: "payload 1",
      key: "key 1",
      partition: 0
    ).wait

    message = wait_for_message(
      topic: TestTopics.consume_test_topic,
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
    new_consumer.subscribe(TestTopics.consume_test_topic)
    wait_for_assignment(new_consumer)

    message.stub(:partition, 9999) do
      assert_raises(Rdkafka::RdkafkaError) do
        new_consumer.store_offset(message)
      end
    end
  ensure
    new_consumer&.close
  end

  def test_position_fetches_the_positions_for_the_current_assignment
    consumer = rdkafka_consumer_config("enable.auto.offset.store": false).consumer

    report = @producer.produce(
      topic: TestTopics.consume_test_topic,
      payload: "payload 1",
      key: "key 1",
      partition: 0
    ).wait

    message = wait_for_message(
      topic: TestTopics.consume_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    # Setup committed state
    consumer.subscribe(TestTopics.consume_test_topic)
    wait_for_assignment(consumer)
    list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
      l.add_topic_and_partitions_with_offsets(TestTopics.consume_test_topic, 0 => 1, 1 => 1, 2 => 1)
    end
    consumer.commit(list)

    consumer.store_offset(message)

    partitions = consumer.position.to_h[TestTopics.consume_test_topic]

    refute_nil partitions
    assert_equal message.offset + 1, partitions[0].offset
  ensure
    consumer&.close
  end

  def test_position_fetches_the_positions_for_a_specified_assignment
    consumer = rdkafka_consumer_config("enable.auto.offset.store": false).consumer

    report = @producer.produce(
      topic: TestTopics.consume_test_topic,
      payload: "payload 1",
      key: "key 1",
      partition: 0
    ).wait

    message = wait_for_message(
      topic: TestTopics.consume_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    # Setup committed state
    consumer.subscribe(TestTopics.consume_test_topic)
    wait_for_assignment(consumer)
    list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
      l.add_topic_and_partitions_with_offsets(TestTopics.consume_test_topic, 0 => 1, 1 => 1, 2 => 1)
    end
    consumer.commit(list)

    consumer.store_offset(message)

    list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
      l.add_topic_and_partitions_with_offsets(TestTopics.consume_test_topic, 0 => nil, 1 => nil, 2 => nil)
    end
    partitions = consumer.position(list).to_h[TestTopics.consume_test_topic]

    refute_nil partitions
    assert_equal message.offset + 1, partitions[0].offset
  ensure
    consumer&.close
  end

  def test_position_raises_an_error_when_getting_the_position_fails
    consumer = rdkafka_consumer_config("enable.auto.offset.store": false).consumer

    # Setup committed state
    handles = []
    10.times do
      (0..2).each do |i|
        handles << @producer.produce(
          topic: TestTopics.consume_test_topic,
          payload: "payload 1",
          key: "key 1",
          partition: i
        )
      end
    end
    handles.each(&:wait)

    consumer.subscribe(TestTopics.consume_test_topic)
    wait_for_assignment(consumer)
    list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
      l.add_topic_and_partitions_with_offsets(TestTopics.consume_test_topic, 0 => 1, 1 => 1, 2 => 1)
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

  def test_store_offset_raises_error_when_auto_offset_store_enabled
    consumer = rdkafka_consumer_config("enable.auto.offset.store": true).consumer

    report = @producer.produce(
      topic: TestTopics.consume_test_topic,
      payload: "payload 1",
      key: "key 1",
      partition: 0
    ).wait

    message = wait_for_message(
      topic: TestTopics.consume_test_topic,
      delivery_report: report,
      consumer: consumer
    )

    # Setup committed state
    consumer.subscribe(TestTopics.consume_test_topic)
    wait_for_assignment(consumer)
    list = Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
      l.add_topic_and_partitions_with_offsets(TestTopics.consume_test_topic, 0 => 1, 1 => 1, 2 => 1)
    end
    consumer.commit(list)

    error = assert_raises(Rdkafka::RdkafkaError) do
      consumer.store_offset(message)
    end
    assert_match(/invalid_arg/, error.message)
  ensure
    consumer&.close
  end

  # -- #query_watermark_offsets --

  def test_query_watermark_offsets_returns_the_watermark_offsets
    # Make sure there's a message
    @producer.produce(
      topic: TestTopics.watermarks_test_topic,
      payload: "payload 1",
      key: "key 1",
      partition: 0
    ).wait

    low, high = @consumer.query_watermark_offsets(TestTopics.watermarks_test_topic, 0, 5000)

    assert_equal 0, low
    assert_operator high, :>, 0
  end

  def test_query_watermark_offsets_raises_an_error_when_querying_fails
    Rdkafka::Bindings.stub(:rd_kafka_query_watermark_offsets, 20) do
      assert_raises(Rdkafka::RdkafkaError) do
        @consumer.query_watermark_offsets(TestTopics.consume_test_topic, 0, 5000)
      end
    end
  end

  # -- #lag --

  def test_lag_calculates_the_consumer_lag
    consumer = rdkafka_consumer_config("enable.partition.eof": true).consumer

    # Make sure there's a message in every partition and
    # wait for the message to make sure everything is committed.
    (0..2).each do |i|
      @producer.produce(
        topic: TestTopics.consume_test_topic,
        key: "key lag #{i}",
        partition: i
      ).wait
    end

    # Consume to the end
    consumer.subscribe(TestTopics.consume_test_topic)
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

    # Create list to fetch lag for
    list = consumer.committed(Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
      l.add_topic(TestTopics.consume_test_topic, 0..2)
    end)

    # Lag should be 0 now
    lag = consumer.lag(list)
    expected_lag = {
      TestTopics.consume_test_topic => {
        0 => 0,
        1 => 0,
        2 => 0
      }
    }

    assert_equal expected_lag, lag

    # Produce message on every topic again
    (0..2).each do |i|
      @producer.produce(
        topic: TestTopics.consume_test_topic,
        key: "key lag #{i}",
        partition: i
      ).wait
    end

    # Lag should be 1 now
    lag = consumer.lag(list)
    expected_lag = {
      TestTopics.consume_test_topic => {
        0 => 1,
        1 => 1,
        2 => 1
      }
    }

    assert_equal expected_lag, lag
  ensure
    consumer&.close
  end

  def test_lag_returns_nil_if_there_are_no_messages_on_the_topic
    consumer = rdkafka_consumer_config("enable.partition.eof": true).consumer

    list = consumer.committed(Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
      l.add_topic(TestTopics.consume_test_topic, 0..2)
    end)

    lag = consumer.lag(list)
    expected_lag = {
      TestTopics.consume_test_topic => {}
    }

    assert_equal expected_lag, lag
  ensure
    consumer&.close
  end

  # -- #cluster_id --

  def test_cluster_id_returns_the_current_cluster_id
    @consumer.subscribe(TestTopics.consume_test_topic)
    wait_for_assignment(@consumer)

    refute_empty @consumer.cluster_id
  end

  # -- #member_id --

  def test_member_id_returns_the_current_member_id
    @consumer.subscribe(TestTopics.consume_test_topic)
    wait_for_assignment(@consumer)

    assert @consumer.member_id.start_with?("rdkafka-")
  end

  # -- #poll --

  def test_poll_returns_nil_if_there_is_no_subscription
    assert_nil @consumer.poll(1000)
  end

  def test_poll_returns_nil_if_there_are_no_messages
    @consumer.subscribe(TestTopics.empty_test_topic)

    assert_nil @consumer.poll(1000)
  end

  def test_poll_returns_a_message_if_there_is_one
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

  def test_poll_raises_an_error_when_polling_fails
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

  # -- #poll_nb --

  def test_poll_nb_returns_nil_if_there_is_no_subscription
    assert_nil @consumer.poll_nb
  end

  def test_poll_nb_returns_nil_if_there_are_no_messages
    @consumer.subscribe(TestTopics.empty_test_topic)

    assert_nil @consumer.poll_nb
  end

  def test_poll_nb_accepts_a_timeout_parameter
    @consumer.subscribe(TestTopics.empty_test_topic)

    assert_nil @consumer.poll_nb(0)
    assert_nil @consumer.poll_nb(100)
  end

  def test_poll_nb_returns_a_message_if_there_is_one
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

  def test_poll_nb_raises_an_error_when_polling_fails
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

  def test_poll_nb_raises_closed_consumer_error_when_closed
    @consumer.close
    error = assert_raises(Rdkafka::ClosedConsumerError) do
      @consumer.poll_nb
    end
    assert_match(/poll_nb/, error.message)
  end

  # -- #poll with headers --

  def test_poll_returns_message_with_headers_using_symbol_keys
    report = @producer.produce(
      topic: TestTopics.consume_test_topic,
      key: "key headers",
      headers: { foo: "bar" }
    ).wait

    message = wait_for_message(topic: TestTopics.consume_test_topic, consumer: @consumer, delivery_report: report)

    refute_nil message
    assert_equal "key headers", message.key
    assert_includes message.headers, "foo"
    assert_equal "bar", message.headers["foo"]
  end

  def test_poll_returns_message_with_headers_using_string_keys
    report = @producer.produce(
      topic: TestTopics.consume_test_topic,
      key: "key headers",
      headers: { "foo" => "bar" }
    ).wait

    message = wait_for_message(topic: TestTopics.consume_test_topic, consumer: @consumer, delivery_report: report)

    refute_nil message
    assert_equal "key headers", message.key
    assert_includes message.headers, "foo"
    assert_equal "bar", message.headers["foo"]
  end

  def test_poll_returns_message_with_no_headers
    report = @producer.produce(
      topic: TestTopics.consume_test_topic,
      key: "key no headers",
      headers: nil
    ).wait

    message = wait_for_message(topic: TestTopics.consume_test_topic, consumer: @consumer, delivery_report: report)

    refute_nil message
    assert_equal "key no headers", message.key
    assert_empty message.headers
  end

  def test_poll_raises_an_error_when_message_headers_arent_readable
    report = @producer.produce(
      topic: TestTopics.consume_test_topic,
      key: "key err headers",
      headers: nil
    ).wait

    Rdkafka::Bindings.stub(:rd_kafka_message_headers, 1) do
      err = assert_raises(Rdkafka::RdkafkaError) do
        wait_for_message(topic: TestTopics.consume_test_topic, consumer: @consumer, delivery_report: report)
      end
      assert err.message.start_with?("Error reading message headers")
    end
  end

  def test_poll_raises_an_error_when_the_first_message_header_isnt_readable
    report = @producer.produce(
      topic: TestTopics.consume_test_topic,
      key: "key err headers",
      headers: { foo: "bar" }
    ).wait

    Rdkafka::Bindings.stub(:rd_kafka_header_get_all, 1) do
      err = assert_raises(Rdkafka::RdkafkaError) do
        wait_for_message(topic: TestTopics.consume_test_topic, consumer: @consumer, delivery_report: report)
      end
      assert err.message.start_with?("Error reading a message header at index 0")
    end
  end

  # -- #each --

  def test_each_yields_messages
    handles = []
    10.times do
      handles << @producer.produce(
        topic: TestTopics.consume_test_topic,
        payload: "payload 1",
        key: "key 1",
        partition: 0
      )
    end
    handles.each(&:wait)

    @consumer.subscribe(TestTopics.consume_test_topic)
    # Check the first 10 messages. Then close the consumer, which
    # should break the each loop.
    @consumer.each_with_index do |message, i|
      assert_kind_of Rdkafka::Consumer::Message, message
      break if i == 10
    end
    @consumer.close
  end

  # -- #each_batch --

  def test_each_batch_raises_not_implemented_error
    assert_raises(NotImplementedError) do
      @consumer.each_batch {}
    end
  end

  # -- #offsets_for_times --

  def test_offsets_for_times_raises_when_not_topic_partition_list
    assert_raises(TypeError) { @consumer.offsets_for_times([]) }
  end

  def test_offsets_for_times_raises_an_error_when_offsets_for_times_fails
    tpl = Rdkafka::Consumer::TopicPartitionList.new

    Rdkafka::Bindings.stub(:rd_kafka_offsets_for_times, 7) do
      assert_raises(Rdkafka::RdkafkaError) { @consumer.offsets_for_times(tpl) }
    end
  end

  def test_offsets_for_times_returns_a_topic_partition_list_with_updated_offsets
    timeout = 1000

    @consumer.subscribe(TestTopics.consume_test_topic)

    # 1. partitions are assigned
    wait_for_assignment(@consumer)

    refute_empty @consumer.assignment

    # 2. eat unrelated messages
    while @consumer.poll(timeout) do; end

    # 3. send messages
    [:a, :b, :c].each do |val|
      @producer.produce(
        topic: TestTopics.consume_test_topic,
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
        TestTopics.consume_test_topic,
        [
          [0, message.timestamp]
        ]
      )
    end

    tpl_response = @consumer.offsets_for_times(tpl)

    assert_equal message.offset, tpl_response.to_h[TestTopics.consume_test_topic][0].offset

    @consumer.unsubscribe
  end

  # -- #events_poll --

  def test_events_poll_propagates_stats_on_events_poll_and_not_poll
    stats = []
    config = rdkafka_consumer_config("statistics.interval.ms": 500)
    config.consumer_poll_set = false
    consumer = config.consumer

    Rdkafka::Config.statistics_callback = ->(published) { stats << published }

    consumer.subscribe(TestTopics.consume_test_topic)
    consumer.poll(1_000)

    assert_empty stats
    consumer.events_poll(-1)

    refute_empty stats
  ensure
    Rdkafka::Config.statistics_callback = nil
    consumer&.close
  end

  # -- #events_poll_nb --

  def test_events_poll_nb_returns_the_number_of_events_processed
    config = rdkafka_consumer_config("statistics.interval.ms": 500)
    config.consumer_poll_set = false
    consumer = config.consumer

    Rdkafka::Config.statistics_callback = ->(published) {}

    consumer.subscribe(TestTopics.consume_test_topic)
    result = consumer.events_poll_nb

    assert_kind_of Integer, result
    assert_operator result, :>=, 0
  ensure
    Rdkafka::Config.statistics_callback = nil
    consumer&.close
  end

  def test_events_poll_nb_accepts_a_timeout_parameter
    config = rdkafka_consumer_config("statistics.interval.ms": 500)
    config.consumer_poll_set = false
    consumer = config.consumer

    Rdkafka::Config.statistics_callback = ->(published) {}

    consumer.subscribe(TestTopics.consume_test_topic)

    assert_operator consumer.events_poll_nb(0), :>=, 0
    assert_operator consumer.events_poll_nb(100), :>=, 0
  ensure
    Rdkafka::Config.statistics_callback = nil
    consumer&.close
  end

  def test_events_poll_nb_processes_events_without_releasing_gvl
    stats = []
    config = rdkafka_consumer_config("statistics.interval.ms": 500)
    config.consumer_poll_set = false
    consumer = config.consumer

    Rdkafka::Config.statistics_callback = ->(published) { stats << published }

    consumer.subscribe(TestTopics.consume_test_topic)
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

  # -- #consumer_group_metadata_pointer --

  def test_consumer_group_metadata_pointer_returns_a_pointer
    pointer = @consumer.consumer_group_metadata_pointer

    assert_kind_of FFI::Pointer, pointer
  ensure
    Rdkafka::Bindings.rd_kafka_consumer_group_metadata_destroy(pointer) if pointer
  end

  # -- a rebalance listener --

  def test_rebalance_listener_gets_notifications
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

    notify_listener(listener)

    assert_equal [
      [:assign, TestTopics.consume_test_topic, 0, 1, 2],
      [:revoke, TestTopics.consume_test_topic, 0, 1, 2]
    ], listener.queue
  end

  def test_rebalance_listener_handles_callback_exceptions
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

    notify_listener(listener)

    assert_equal [:assigned, :revoked], listener.queue
  end

  # -- methods that should not be called after a consumer has been closed --

  def test_closed_consumer_raises_on_subscribe
    @consumer.close
    error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.subscribe(nil) }
    assert_match(/subscribe/, error.message)
  end

  def test_closed_consumer_raises_on_unsubscribe
    @consumer.close
    error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.unsubscribe }
    assert_match(/unsubscribe/, error.message)
  end

  def test_closed_consumer_raises_on_pause
    @consumer.close
    error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.pause(nil) }
    assert_match(/pause/, error.message)
  end

  def test_closed_consumer_raises_on_resume
    @consumer.close
    error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.resume(nil) }
    assert_match(/resume/, error.message)
  end

  def test_closed_consumer_raises_on_subscription
    @consumer.close
    error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.subscription }
    assert_match(/subscription/, error.message)
  end

  def test_closed_consumer_raises_on_assign
    @consumer.close
    error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.assign(nil) }
    assert_match(/assign/, error.message)
  end

  def test_closed_consumer_raises_on_assignment
    @consumer.close
    error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.assignment }
    assert_match(/assignment/, error.message)
  end

  def test_closed_consumer_raises_on_committed
    @consumer.close
    error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.committed }
    assert_match(/committed/, error.message)
  end

  def test_closed_consumer_raises_on_query_watermark_offsets
    @consumer.close
    error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.query_watermark_offsets(nil, nil) }
    assert_match(/query_watermark_offsets/, error.message)
  end

  def test_closed_consumer_raises_on_assignment_lost
    @consumer.close
    error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.assignment_lost? }
    assert_match(/assignment_lost\?/, error.message)
  end

  def test_closed_consumer_raises_on_poll_nb
    @consumer.close
    error = assert_raises(Rdkafka::ClosedConsumerError) { @consumer.poll_nb }
    assert_match(/poll_nb/, error.message)
  end

  # -- finalizer --

  def test_provides_a_finalizer_that_closes_the_native_kafka_client
    refute_predicate @consumer, :closed?

    @consumer.finalizer.call("some-ignored-object-id")

    assert_predicate @consumer, :closed?
  end

  # -- cooperative rebalance protocol --

  def test_cooperative_rebalance_protocol_assigns_and_unassigns_partitions
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

    notify_listener(listener) do
      handles = []
      10.times do
        handles << @producer.produce(
          topic: TestTopics.consume_test_topic,
          payload: "payload 1",
          key: "key 1",
          partition: 0
        )
      end
      handles.each(&:wait)

      @consumer.subscribe(TestTopics.consume_test_topic)
      # Check the first 10 messages. Then close the consumer, which
      # should break the each loop.
      @consumer.each_with_index do |message, i|
        assert_kind_of Rdkafka::Consumer::Message, message
        break if i == 10
      end
    end

    assert_equal [
      [:assign, TestTopics.consume_test_topic, 0, 1, 2],
      [:revoke, TestTopics.consume_test_topic, 0, 1, 2]
    ], listener.queue
  end

  # -- #oauthbearer_set_token --

  def test_oauthbearer_set_token_when_sasl_not_configured
    response = @consumer.oauthbearer_set_token(
      token: "foo",
      lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
      principal_name: "kafka-cluster"
    )

    assert_equal Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE, response
  end

  def test_oauthbearer_set_token_when_sasl_configured_without_extensions
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

  def test_oauthbearer_set_token_when_sasl_configured_with_extensions
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

  # -- eof on topic with eof reporting enabled --

  def test_eof_reporting_returns_proper_details
    consumer = rdkafka_consumer_config("enable.partition.eof": true).consumer

    (0..2).each do |i|
      @producer.produce(
        topic: TestTopics.consume_test_topic,
        key: "key lag #{i}",
        partition: i
      ).wait
    end

    # Consume to the end
    consumer.subscribe(TestTopics.consume_test_topic)
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

  # -- long running consumption --

  def test_consumes_messages_continuously_for_60_seconds
    @consumer.subscribe(TestTopics.consume_test_topic)
    wait_for_assignment(@consumer)

    messages_consumed = 0
    start_time = Time.now

    # Producer thread - sends message every second
    producer_thread = Thread.new do
      counter = 0
      while Time.now - start_time < 60
        @producer.produce(
          topic: TestTopics.consume_test_topic,
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
        assert_equal TestTopics.consume_test_topic, message.topic
        messages_consumed += 1
        @consumer.commit if messages_consumed % 10 == 0
      end
    end

    producer_thread.join

    assert_operator messages_consumed, :>, 50 # Should consume most messages
  end

  # -- #events_poll_nb_each --

  def test_events_poll_nb_each_does_not_raise_when_queue_is_empty
    @consumer.events_poll_nb_each { |_| }
  end

  def test_events_poll_nb_each_yields_the_count_after_each_poll
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

  def test_events_poll_nb_each_stops_when_block_returns_stop
    iterations = 0

    Rdkafka::Bindings.stub(:rd_kafka_poll_nb, 1) do
      @consumer.events_poll_nb_each do |_count|
        iterations += 1
        :stop if iterations >= 3
      end
    end

    assert_equal 3, iterations
  end

  def test_events_poll_nb_each_raises_closed_consumer_error_when_closed
    @consumer.close
    error = assert_raises(Rdkafka::ClosedConsumerError) do
      @consumer.events_poll_nb_each { |_| }
    end
    assert_match(/events_poll_nb_each/, error.message)
  end

  # -- #poll_nb_each --

  def test_poll_nb_each_does_not_raise_when_queue_is_empty
    @consumer.subscribe(TestTopics.consume_test_topic)
    # Give it a moment to subscribe
    sleep 0.5

    messages = []
    @consumer.poll_nb_each { |msg| messages << msg }

    assert_kind_of Array, messages
  end

  def test_poll_nb_each_yields_messages_and_respects_stop
    topic = TestTopics.consume_test_topic
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

    # Now test that :stop works - we should get exactly one more message then stop
    # (we already consumed one with blocking poll above)
    messages = []
    @consumer.poll_nb_each do |message|
      messages << message
      :stop if messages.size >= 1
    end

    # Should have stopped after exactly 1 message (we got 1 via blocking poll earlier)
    assert_equal 1, messages.size
  end

  def test_poll_nb_each_properly_cleans_up_message_pointers
    topic = TestTopics.consume_test_topic
    @consumer.subscribe(topic)

    @producer.produce(topic: topic, payload: "cleanup test")
    @producer.flush
    sleep 2

    # This should not leak memory - message_destroy is called in ensure
    @consumer.poll_nb_each { |_| }
  end

  def test_poll_nb_each_raises_closed_consumer_error_when_closed
    @consumer.close
    error = assert_raises(Rdkafka::ClosedConsumerError) do
      @consumer.poll_nb_each { |_| }
    end
    assert_match(/poll_nb_each/, error.message)
  end

  # -- file descriptor access for fiber scheduler integration --

  def test_enables_io_events_on_consumer_queue
    @consumer.subscribe(TestTopics.consume_test_topic)

    signal_r, signal_w = IO.pipe
    @consumer.enable_queue_io_events(signal_w.fileno)
    signal_r.close
    signal_w.close
  end

  def test_enables_io_events_on_background_queue
    @consumer.subscribe(TestTopics.consume_test_topic)

    signal_r, signal_w = IO.pipe
    @consumer.enable_background_queue_io_events(signal_w.fileno)
    signal_r.close
    signal_w.close
  end

  def test_enables_fd_with_payload_option
    @consumer.subscribe(TestTopics.consume_test_topic)

    signal_r, signal_w = IO.pipe
    custom_payload = "hello"
    @consumer.enable_queue_io_events(signal_w.fileno, custom_payload)
    signal_r.close
    signal_w.close
  end

  def test_supports_normal_polling_with_io_events_enabled
    topic = TestTopics.consume_test_topic
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

  def test_closed_consumer_raises_closed_inner_error_on_enable_queue_io_events
    @consumer.close
    signal_r, signal_w = IO.pipe
    assert_raises(Rdkafka::ClosedInnerError) do
      @consumer.enable_queue_io_events(signal_w.fileno)
    end
    signal_r.close
    signal_w.close
  end

  def test_closed_consumer_raises_closed_inner_error_on_enable_background_queue_io_events
    @consumer.close
    signal_r, signal_w = IO.pipe
    assert_raises(Rdkafka::ClosedInnerError) do
      @consumer.enable_background_queue_io_events(signal_w.fileno)
    end
    signal_r.close
    signal_w.close
  end
end
