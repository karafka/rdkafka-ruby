require "spec_helper"
require "ostruct"

describe Rdkafka::Consumer do
  let(:config) { rdkafka_config }
  let(:consumer) { config.consumer }
  let(:producer) { config.producer }

  after do
    consumer.close
    producer.close
  end

  describe "#subscripe, #unsubscribe and #subscription" do
    it "should subscribe, unsubscribe and return the subscription" do
      expect(consumer.subscription).to be_empty

      consumer.subscribe("consume_test_topic")

      expect(consumer.subscription).not_to be_empty
      expected_subscription = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic("consume_test_topic")
      end
      expect(consumer.subscription).to eq expected_subscription

      consumer.unsubscribe

      expect(consumer.subscription).to be_empty
    end

    it "should raise an error when subscribing fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_subscribe).and_return(20)

      expect {
        consumer.subscribe("consume_test_topic")
      }.to raise_error(Rdkafka::RdkafkaError)
    end

    it "should raise an error when unsubscribing fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_unsubscribe).and_return(20)

      expect {
        consumer.unsubscribe
      }.to raise_error(Rdkafka::RdkafkaError)
    end

    it "should raise an error when fetching the subscription fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_subscription).and_return(20)

      expect {
        consumer.subscription
      }.to raise_error(Rdkafka::RdkafkaError)
    end
  end

  describe "#pause and #resume" do
    context "subscription" do
      let(:timeout) { 1000 }

      before { consumer.subscribe("consume_test_topic") }

      it "should pause and then resume" do
        # 1. partitions are assigned
        wait_for_assignment(consumer)
        expect(consumer.assignment).not_to be_empty

        # 2. send a first message
        send_one_message

        # 3. ensure that message is successfully consumed
        records = consumer.poll(timeout)
        expect(records).not_to be_nil
        consumer.commit

        # 4. send a second message
        send_one_message

        # 5. pause the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic("consume_test_topic", (0..2))
        consumer.pause(tpl)

        # 6. ensure that messages are not available
        records = consumer.poll(timeout)
        expect(records).to be_nil

        # 7. resume the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic("consume_test_topic", (0..2))
        consumer.resume(tpl)

        # 8. ensure that message is successfully consumed
        records = consumer.poll(timeout)
        expect(records).not_to be_nil
        consumer.commit
      end
    end

    it "should raise when not TopicPartitionList" do
      expect { consumer.pause(true) }.to raise_error(TypeError)
      expect { consumer.resume(true) }.to raise_error(TypeError)
    end

    it "should raise an error when pausing fails" do
      list = Rdkafka::Consumer::TopicPartitionList.new.tap { |tpl| tpl.add_topic('topic', (0..1)) }

      expect(Rdkafka::Bindings).to receive(:rd_kafka_pause_partitions).and_return(20)
      expect {
        consumer.pause(list)
      }.to raise_error do |err|
        expect(err).to be_instance_of(Rdkafka::RdkafkaTopicPartitionListError)
        expect(err.topic_partition_list).to be
      end
    end

    it "should raise an error when resume fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_resume_partitions).and_return(20)
      expect {
        consumer.resume(Rdkafka::Consumer::TopicPartitionList.new)
      }.to raise_error Rdkafka::RdkafkaError
    end

    def send_one_message
      producer.produce(
        topic:     "consume_test_topic",
        payload:   "payload 1",
        key:       "key 1"
      ).wait
    end
  end

  describe "#seek" do
    it "should raise an error when seeking fails" do
      fake_msg = OpenStruct.new(topic: "consume_test_topic", partition: 0, offset: 0)

      expect(Rdkafka::Bindings).to receive(:rd_kafka_seek).and_return(20)
      expect {
        consumer.seek(fake_msg)
      }.to raise_error Rdkafka::RdkafkaError
    end

    context "subscription" do
      let(:timeout) { 1000 }

      before do
        consumer.subscribe("consume_test_topic")

        # 1. partitions are assigned
        wait_for_assignment(consumer)
        expect(consumer.assignment).not_to be_empty

        # 2. eat unrelated messages
        while(consumer.poll(timeout)) do; end
      end

      def send_one_message(val)
        producer.produce(
          topic:     "consume_test_topic",
          payload:   "payload #{val}",
          key:       "key 1",
          partition: 0
        ).wait
      end

      it "works when a partition is paused" do
        # 3. get reference message
        send_one_message(:a)
        message1 = consumer.poll(timeout)
        expect(message1&.payload).to eq "payload a"

        # 4. pause the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic("consume_test_topic", 1)
        consumer.pause(tpl)

        # 5. seek to previous message
        consumer.seek(message1)

        # 6. resume the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic("consume_test_topic", 1)
        consumer.resume(tpl)

        # 7. ensure same message is read again
        message2 = consumer.poll(timeout)
        consumer.commit
        expect(message1.offset).to eq message2.offset
        expect(message1.payload).to eq message2.payload
      end

      it "allows skipping messages" do
        # 3. send messages
        send_one_message(:a)
        send_one_message(:b)
        send_one_message(:c)

        # 4. get reference message
        message = consumer.poll(timeout)
        expect(message&.payload).to eq "payload a"

        # 5. seek over one message
        fake_msg = message.dup
        fake_msg.instance_variable_set(:@offset, fake_msg.offset + 2)
        consumer.seek(fake_msg)

        # 6. ensure that only one message is available
        records = consumer.poll(timeout)
        expect(records&.payload).to eq "payload c"
        records = consumer.poll(timeout)
        expect(records).to be_nil

        consumer.commit
      end
    end
  end

  describe "#assign and #assignment" do
    it "should return an empty assignment if nothing is assigned" do
      expect(consumer.assignment).to be_empty
    end

    it "should only accept a topic partition list in assign" do
      expect {
        consumer.assign("list")
      }.to raise_error TypeError
    end

    it "should raise an error when assigning fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_assign).and_return(20)
      expect {
        consumer.assign(Rdkafka::Consumer::TopicPartitionList.new)
      }.to raise_error Rdkafka::RdkafkaError
    end

    it "should assign specific topic/partitions and return that assignment" do
      tpl = Rdkafka::Consumer::TopicPartitionList.new
      tpl.add_topic("consume_test_topic", (0..2))
      consumer.assign(tpl)

      assignment = consumer.assignment
      expect(assignment).not_to be_empty
      expect(assignment.to_h["consume_test_topic"].length).to eq 3
    end

    it "should return the assignment when subscribed" do
      # Make sure there's a message
      report = producer.produce(
        topic:     "consume_test_topic",
        payload:   "payload 1",
        key:       "key 1",
        partition: 0
      ).wait

      # Subscribe and poll until partitions are assigned
      consumer.subscribe("consume_test_topic")
      100.times do
        consumer.poll(100)
        break unless consumer.assignment.empty?
      end

      assignment = consumer.assignment
      expect(assignment).not_to be_empty
      expect(assignment.to_h["consume_test_topic"].length).to eq 3
    end

    it "should raise an error when getting assignment fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_assignment).and_return(20)
      expect {
        consumer.assignment
      }.to raise_error Rdkafka::RdkafkaError
    end
  end

  describe "#close" do
    it "should close a consumer" do
      consumer.subscribe("consume_test_topic")
      consumer.close
      expect(consumer.poll(100)).to be_nil
    end
  end

  describe "#commit, #committed and #store_offset" do
    # Make sure there's a stored offset
    let!(:report) do
      report = producer.produce(
        topic:     "consume_test_topic",
        payload:   "payload 1",
        key:       "key 1",
        partition: 0
      ).wait
    end

    let(:message) do
      wait_for_message(
        topic: "consume_test_topic",
        delivery_report: report,
        consumer: consumer
      )
    end

    it "should only accept a topic partition list in committed" do
      expect {
        consumer.committed("list")
      }.to raise_error TypeError
    end

    it "should commit in sync mode" do
      expect {
        consumer.commit(nil, true)
      }.not_to raise_error
    end

    it "should only accept a topic partition list in commit if not nil" do
      expect {
        consumer.commit("list")
      }.to raise_error TypeError
    end

    context "with a commited consumer" do
      before :all do
        # Make sure there are some message
        producer = rdkafka_config.producer
        handles = []
        10.times do
          (0..2).each do |i|
            handles << producer.produce(
              topic:     "consume_test_topic",
              payload:   "payload 1",
              key:       "key 1",
              partition: i
            )
          end
        end
        handles.each(&:wait)
        producer.close
      end

      before do
        consumer.subscribe("consume_test_topic")
        wait_for_assignment(consumer)
        list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
          list.add_topic_and_partitions_with_offsets("consume_test_topic", 0 => 1, 1 => 1, 2 => 1)
        end
        consumer.commit(list)
      end

      it "should commit a specific topic partion list" do
        list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
          list.add_topic_and_partitions_with_offsets("consume_test_topic", 0 => 1, 1 => 2, 2 => 3)
        end
        consumer.commit(list)

        partitions = consumer.committed(list).to_h["consume_test_topic"]
        expect(partitions[0].offset).to eq 1
        expect(partitions[1].offset).to eq 2
        expect(partitions[2].offset).to eq 3
      end

      it "should raise an error when committing fails" do
        expect(Rdkafka::Bindings).to receive(:rd_kafka_commit).and_return(20)

        expect {
          consumer.commit
        }.to raise_error(Rdkafka::RdkafkaError)
      end

      it "should fetch the committed offsets for the current assignment" do
        partitions = consumer.committed.to_h["consume_test_topic"]
        expect(partitions).not_to be_nil
        expect(partitions[0].offset).to eq 1
      end

      it "should fetch the committed offsets for a specified topic partition list" do
        list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
          list.add_topic("consume_test_topic", [0, 1, 2])
        end
        partitions = consumer.committed(list).to_h["consume_test_topic"]
        expect(partitions).not_to be_nil
        expect(partitions[0].offset).to eq 1
        expect(partitions[1].offset).to eq 1
        expect(partitions[2].offset).to eq 1
      end

      it "should raise an error when getting committed fails" do
        expect(Rdkafka::Bindings).to receive(:rd_kafka_committed).and_return(20)
        list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
          list.add_topic("consume_test_topic", [0, 1, 2])
        end
        expect {
          consumer.committed(list)
        }.to raise_error Rdkafka::RdkafkaError
      end

      describe "#store_offset" do
        before do
          config[:'enable.auto.offset.store'] = false
          config[:'enable.auto.commit'] = false
          consumer.subscribe("consume_test_topic")
          wait_for_assignment(consumer)
        end

        it "should store the offset for a message" do
          consumer.store_offset(message)
          consumer.commit

          list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
            list.add_topic("consume_test_topic", [0, 1, 2])
          end
          partitions = consumer.committed(list).to_h["consume_test_topic"]
          expect(partitions).not_to be_nil
          expect(partitions[message.partition].offset).to eq(message.offset + 1)
        end

        it "should raise an error with invalid input" do
          allow(message).to receive(:partition).and_return(9999)
          expect {
            consumer.store_offset(message)
          }.to raise_error Rdkafka::RdkafkaError
        end
      end
    end
  end

  describe "#query_watermark_offsets" do
    it "should return the watermark offsets" do
      # Make sure there's a message
      producer.produce(
        topic:     "watermarks_test_topic",
        payload:   "payload 1",
        key:       "key 1",
        partition: 0
      ).wait

      low, high = consumer.query_watermark_offsets("watermarks_test_topic", 0, 5000)
      expect(low).to eq 0
      expect(high).to be > 0
    end

    it "should raise an error when querying offsets fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_query_watermark_offsets).and_return(20)
      expect {
        consumer.query_watermark_offsets("consume_test_topic", 0, 5000)
      }.to raise_error Rdkafka::RdkafkaError
    end
  end

  describe "#lag" do
    let(:config) { rdkafka_config(:"enable.partition.eof" => true) }

    it "should calculate the consumer lag" do
      # Make sure there's a message in every partition and
      # wait for the message to make sure everything is committed.
      (0..2).each do |i|
        report = producer.produce(
          topic:     "consume_test_topic",
          key:       "key lag #{i}",
          partition: i
        ).wait
      end

      # Consume to the end
      consumer.subscribe("consume_test_topic")
      eof_count = 0
      loop do
        begin
          consumer.poll(100)
        rescue Rdkafka::RdkafkaError => error
          if error.is_partition_eof?
            eof_count += 1
          end
          break if eof_count == 3
        end
      end

      # Commit
      consumer.commit

      # Create list to fetch lag for. TODO creating the list will not be necessary
      # after committed uses the subscription.
      list = consumer.committed(Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic("consume_test_topic", (0..2))
      end)

      # Lag should be 0 now
      lag = consumer.lag(list)
      expected_lag = {
        "consume_test_topic" => {
          0 => 0,
          1 => 0,
          2 => 0
        }
      }
      expect(lag).to eq(expected_lag)

      # Produce message on every topic again
      (0..2).each do |i|
        report = producer.produce(
          topic:     "consume_test_topic",
          key:       "key lag #{i}",
          partition: i
        ).wait
      end

      # Lag should be 1 now
      lag = consumer.lag(list)
      expected_lag = {
        "consume_test_topic" => {
          0 => 1,
          1 => 1,
          2 => 1
        }
      }
      expect(lag).to eq(expected_lag)
    end

    it "returns nil if there are no messages on the topic" do
      list = consumer.committed(Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic("consume_test_topic", (0..2))
      end)

      lag = consumer.lag(list)
      expected_lag = {
        "consume_test_topic" => {}
      }
      expect(lag).to eq(expected_lag)
    end
  end

  describe "#cluster_id" do
    it 'should return the current ClusterId' do
      consumer.subscribe("consume_test_topic")
      wait_for_assignment(consumer)
      expect(consumer.cluster_id).not_to be_empty
    end
  end

  describe "#member_id" do
    it 'should return the current MemberId' do
      consumer.subscribe("consume_test_topic")
      wait_for_assignment(consumer)
      expect(consumer.member_id).to start_with('rdkafka-')
    end
  end

  describe "#poll" do
    it "should return nil if there is no subscription" do
      expect(consumer.poll(1000)).to be_nil
    end

    it "should return nil if there are no messages" do
      consumer.subscribe("empty_test_topic")
      expect(consumer.poll(1000)).to be_nil
    end

    it "should return a message if there is one" do
      producer.produce(
        topic:     "consume_test_topic",
        payload:   "payload 1",
        key:       "key 1"
      ).wait

      consumer.subscribe("consume_test_topic")
      message = consumer.poll(5000)
      expect(message).to be_a Rdkafka::Consumer::Message

      # Message content is tested in producer spec
    end

    it "should raise an error when polling fails" do
      message = Rdkafka::Bindings::Message.new.tap do |message|
        message[:err] = 20
      end
      message_pointer = message.to_ptr
      expect(Rdkafka::Bindings).to receive(:rd_kafka_consumer_poll).and_return(message_pointer)
      expect(Rdkafka::Bindings).to receive(:rd_kafka_message_destroy).with(message_pointer)
      expect {
        consumer.poll(100)
      }.to raise_error Rdkafka::RdkafkaError
    end
  end

  describe "#poll with headers" do
    it "should return message with headers" do
      report = producer.produce(
        topic:     "consume_test_topic",
        key:       "key headers",
        headers:   { foo: 'bar' }
      ).wait

      message = wait_for_message(topic: "consume_test_topic", consumer: consumer, delivery_report: report)
      expect(message).to be
      expect(message.key).to eq('key headers')
      expect(message.headers).to include(foo: 'bar')
    end

    it "should return message with no headers" do
      report = producer.produce(
        topic:     "consume_test_topic",
        key:       "key no headers",
        headers:   nil
      ).wait

      message = wait_for_message(topic: "consume_test_topic", consumer: consumer, delivery_report: report)
      expect(message).to be
      expect(message.key).to eq('key no headers')
      expect(message.headers).to be_empty
    end

    it "should raise an error when message headers aren't readable" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_message_headers).with(any_args) { 1 }

      report = producer.produce(
        topic:     "consume_test_topic",
        key:       "key err headers",
        headers:   nil
      ).wait

      expect {
        wait_for_message(topic: "consume_test_topic", consumer: consumer, delivery_report: report)
      }.to raise_error do |err|
        expect(err).to be_instance_of(Rdkafka::RdkafkaError)
        expect(err.message).to start_with("Error reading message headers")
      end
    end

    it "should raise an error when the first message header aren't readable" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_header_get_all).with(any_args) { 1 }

      report = producer.produce(
        topic:     "consume_test_topic",
        key:       "key err headers",
        headers:   { foo: 'bar' }
      ).wait

      expect {
        wait_for_message(topic: "consume_test_topic", consumer: consumer, delivery_report: report)
      }.to raise_error do |err|
        expect(err).to be_instance_of(Rdkafka::RdkafkaError)
        expect(err.message).to start_with("Error reading a message header at index 0")
      end
    end
  end

  describe "#each" do
    it "should yield messages" do
      handles = []
      10.times do
        handles << producer.produce(
          topic:     "consume_test_topic",
          payload:   "payload 1",
          key:       "key 1",
          partition: 0
        )
      end
      handles.each(&:wait)

      consumer.subscribe("consume_test_topic")
      # Check the first 10 messages. Then close the consumer, which
      # should break the each loop.
      consumer.each_with_index do |message, i|
        expect(message).to be_a Rdkafka::Consumer::Message
        consumer.close if i == 10
      end
    end
  end

  describe "a rebalance listener" do
    it "should get notifications" do
      listener = Struct.new(:queue) do
        def on_partitions_assigned(consumer, list)
          collect(:assign, list)
        end

        def on_partitions_revoked(consumer, list)
          collect(:revoke, list)
        end

        def collect(name, list)
          partitions = list.to_h.map { |key, values| [key, values.map(&:partition)] }.flatten
          queue << ([name] + partitions)
        end
      end.new([])

      notify_listener(listener)

      expect(listener.queue).to eq([
        [:assign, "consume_test_topic", 0, 1, 2],
        [:revoke, "consume_test_topic", 0, 1, 2]
      ])
    end

    it 'should handle callback exceptions' do
      listener = Struct.new(:queue) do
        def on_partitions_assigned(consumer, list)
          queue << :assigned
          raise 'boom'
        end

        def on_partitions_revoked(consumer, list)
          queue << :revoked
          raise 'boom'
        end
      end.new([])

      notify_listener(listener)

      expect(listener.queue).to eq([:assigned, :revoked])
    end

    def notify_listener(listener)
      # 1. subscribe and poll
      config.consumer_rebalance_listener = listener
      consumer.subscribe("consume_test_topic")
      wait_for_assignment(consumer)
      consumer.poll(100)

      # 2. unsubscribe
      consumer.unsubscribe
      wait_for_unassignment(consumer)
      consumer.close
    end
  end
end
