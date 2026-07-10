# frozen_string_literal: true

require "ostruct"
require "securerandom"

RSpec.describe Rdkafka::Consumer do
  let(:consumer) { rdkafka_consumer_config.consumer }
  let(:producer) { rdkafka_producer_config.producer }
  let(:topic) { TestTopics.create }

  after {
    consumer.close
    producer.close
  }

  describe "#name" do
    it { expect(consumer.name).to include("rdkafka#consumer-") }
  end

  describe "consumer without auto-start" do
    let(:consumer) { rdkafka_consumer_config.consumer(native_kafka_auto_start: false) }

    it "expect to be able to start it later and close" do
      consumer.start
      consumer.close
    end

    it "expect to be able to close it without starting" do
      consumer.close
    end
  end

  describe "#subscribe, #unsubscribe and #subscription" do
    it "subscribe,s unsubscribe and return the subscription" do
      expect(consumer.subscription).to be_empty

      consumer.subscribe(topic)

      expect(consumer.subscription).not_to be_empty
      expected_subscription = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic(topic)
      end
      expect(consumer.subscription).to eq expected_subscription

      consumer.unsubscribe

      expect(consumer.subscription).to be_empty
    end

    it "raises an error when subscribing fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_subscribe).and_return(20)

      expect {
        consumer.subscribe(topic)
      }.to raise_error(Rdkafka::RdkafkaError)
    end

    it "raises an error when unsubscribing fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_unsubscribe).and_return(20)

      expect {
        consumer.unsubscribe
      }.to raise_error(Rdkafka::RdkafkaError)
    end

    it "raises an error when fetching the subscription fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_subscription).and_return(20)

      expect {
        consumer.subscription
      }.to raise_error(Rdkafka::RdkafkaError)
    end

    context "when using consumer without the poll set" do
      let(:consumer) do
        config = rdkafka_consumer_config
        config.consumer_poll_set = false
        config.consumer
      end

      it "subscribe,s unsubscribe and return the subscription" do
        expect(consumer.subscription).to be_empty

        consumer.subscribe(topic)

        expect(consumer.subscription).not_to be_empty
        expected_subscription = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
          list.add_topic(topic)
        end
        expect(consumer.subscription).to eq expected_subscription

        consumer.unsubscribe

        expect(consumer.subscription).to be_empty
      end
    end
  end

  describe "#pause and #resume" do
    context "subscription" do
      let(:timeout) { 2000 }

      before { consumer.subscribe(topic) }
      after { consumer.unsubscribe }

      it "pauses and then resume" do
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
        tpl.add_topic(topic, 0..2)
        consumer.pause(tpl)

        # 6. ensure that messages are not available
        records = consumer.poll(timeout)
        expect(records).to be_nil

        # 7. resume the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic(topic, 0..2)
        consumer.resume(tpl)

        # 8. ensure that message is successfully consumed
        records = consumer.poll(timeout)
        expect(records).not_to be_nil
      end
    end

    it "raises when not TopicPartitionList" do
      expect { consumer.pause(true) }.to raise_error(TypeError)
      expect { consumer.resume(true) }.to raise_error(TypeError)
    end

    it "raises an error when pausing fails" do
      list = Rdkafka::Consumer::TopicPartitionList.new.tap { |tpl| tpl.add_topic(TestTopics.unique, 0..1) }

      expect(Rdkafka::Bindings).to receive(:rd_kafka_pause_partitions).and_return(20)
      expect {
        consumer.pause(list)
      }.to raise_error do |err|
        expect(err).to be_instance_of(Rdkafka::RdkafkaTopicPartitionListError)
        expect(err.topic_partition_list).to be
      end
    end

    it "raises an error when resume fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_resume_partitions).and_return(20)
      expect {
        consumer.resume(Rdkafka::Consumer::TopicPartitionList.new)
      }.to raise_error Rdkafka::RdkafkaError
    end

    def send_one_message
      producer.produce(
        topic: topic,
        payload: "payload 1",
        key: "key 1"
      ).wait
    end
  end

  describe "#seek" do
    let(:topic) { TestTopics.unique }

    before do
      admin = rdkafka_producer_config.admin
      admin.create_topic(topic, 1, 1).wait
      admin.close
    end

    it "raises an error when seeking fails" do
      fake_msg = OpenStruct.new(topic: topic, partition: 0, offset: 0)

      expect(Rdkafka::Bindings).to receive(:rd_kafka_seek).and_return(20)
      expect {
        consumer.seek(fake_msg)
      }.to raise_error Rdkafka::RdkafkaError
    end

    context "subscription" do
      let(:timeout) { 1000 }
      # Some specs here test the manual offset commit hence we want to ensure, that we have some
      # offsets in-memory that we can manually commit
      let(:consumer) { rdkafka_consumer_config("auto.commit.interval.ms": 60_000).consumer }

      before do
        consumer.subscribe(topic)

        # 1. partitions are assigned
        wait_for_assignment(consumer)
        expect(consumer.assignment).not_to be_empty

        # 2. eat unrelated messages
        while consumer.poll(timeout) do; end
      end

      after { consumer.unsubscribe }

      def send_one_message(val)
        producer.produce(
          topic: topic,
          payload: "payload #{val}",
          key: "key 1",
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
      end
    end
  end

  describe "#seek_by" do
    let(:consumer) { rdkafka_consumer_config("auto.commit.interval.ms": 60_000).consumer }
    let(:topic) { TestTopics.unique }
    let(:partition) { 0 }
    let(:offset) { 0 }

    before do
      admin = rdkafka_producer_config.admin
      admin.create_topic(topic, 1, 1).wait
      admin.close
    end

    it "raises an error when seeking fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_seek).and_return(20)
      expect {
        consumer.seek_by(topic, partition, offset)
      }.to raise_error Rdkafka::RdkafkaError
    end

    context "subscription" do
      let(:timeout) { 1000 }

      before do
        consumer.subscribe(topic)

        # 1. partitions are assigned
        wait_for_assignment(consumer)
        expect(consumer.assignment).not_to be_empty

        # 2. eat unrelated messages
        while consumer.poll(timeout) do; end
      end

      after { consumer.unsubscribe }

      def send_one_message(val)
        producer.produce(
          topic: topic,
          payload: "payload #{val}",
          key: "key 1",
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
        consumer.seek_by(message.topic, message.partition, message.offset + 2)

        # 6. ensure that only one message is available
        records = consumer.poll(timeout)
        expect(records&.payload).to eq "payload c"
        records = consumer.poll(timeout)
        expect(records).to be_nil
      end
    end
  end

  describe "#assign and #assignment" do
    it "returns an empty assignment if nothing is assigned" do
      expect(consumer.assignment).to be_empty
    end

    it "onlies accept a topic partition list in assign" do
      expect {
        consumer.assign("list")
      }.to raise_error TypeError
    end

    it "raises an error when assigning fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_assign).and_return(20)
      expect {
        consumer.assign(Rdkafka::Consumer::TopicPartitionList.new)
      }.to raise_error Rdkafka::RdkafkaError
    end

    it "assigns specific topic/partitions and return that assignment" do
      tpl = Rdkafka::Consumer::TopicPartitionList.new
      tpl.add_topic(topic, 0..2)
      consumer.assign(tpl)

      assignment = consumer.assignment
      expect(assignment).not_to be_empty
      expect(assignment.to_h[topic].length).to eq 3
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
      expect(assignment).not_to be_empty
      expect(assignment.to_h[topic].length).to eq 3
    end

    it "raises an error when getting assignment fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_assignment).and_return(20)
      expect {
        consumer.assignment
      }.to raise_error Rdkafka::RdkafkaError
    end
  end

  describe "#assignment_lost?" do
    it "does not return true as we do have an assignment" do
      consumer.subscribe(topic)
      Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic(topic)
      end

      expect(consumer.assignment_lost?).to be false
      consumer.unsubscribe
    end

    it "does not return true after voluntary unsubscribing" do
      consumer.subscribe(topic)
      Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic(topic)
      end

      consumer.unsubscribe
      expect(consumer.assignment_lost?).to be false
    end
  end

  describe ".finalizer" do
    let(:native_kafka) { instance_double(Rdkafka::NativeKafka) }
    let(:inner) { double("inner native handle") }

    before do
      allow(native_kafka).to receive(:closed?).and_return(false)
      allow(native_kafka).to receive(:synchronize).and_yield(inner)
      allow(native_kafka).to receive(:close)
      allow(Rdkafka::Bindings).to receive(:rd_kafka_consumer_close)
      allow(Rdkafka::Bindings).to receive(:rd_kafka_queue_destroy)
    end

    it "closes the consumer, destroys the consumer queue, then closes the native client" do
      queue = FFI::MemoryPointer.new(:int)

      described_class.finalizer(native_kafka, [queue]).call

      expect(Rdkafka::Bindings).to have_received(:rd_kafka_consumer_close).with(inner)
      expect(Rdkafka::Bindings).to have_received(:rd_kafka_queue_destroy).with(queue)
      expect(native_kafka).to have_received(:close)
    end

    it "does not destroy a queue that was never created" do
      described_class.finalizer(native_kafka, []).call

      expect(Rdkafka::Bindings).not_to have_received(:rd_kafka_queue_destroy)
      expect(native_kafka).to have_received(:close)
    end

    it "does nothing when the native client is already closed" do
      allow(native_kafka).to receive(:closed?).and_return(true)

      described_class.finalizer(native_kafka, [FFI::MemoryPointer.new(:int)]).call

      expect(native_kafka).not_to have_received(:synchronize)
      expect(native_kafka).not_to have_received(:close)
    end
  end

  # End-to-end reproduction of the leak this branch fixes, against a real consumer and broker.
  # A consumer that used poll_batch holds a real consumer-queue reference; the finalizer the
  # consumer actually registers (what GC runs) must destroy that exact pointer and close the
  # consumer. On the previous code (generic NativeKafka finalizer) the pointer was never
  # destroyed and rd_kafka_consumer_close was skipped, so this example fails there.
  describe "GC finalizer (real consumer-queue reproduction)" do
    it "destroys the exact consumer-queue pointer poll_batch acquired and closes the consumer" do
      acquired = []
      # Only what the finalizer itself does is recorded. Matching native frees by pointer
      # address across the whole example would be unreliable: the allocator can recycle an
      # address from an unrelated queue freed earlier, so a global match is flaky. The
      # finalizer is the only place a Ruby-level rd_kafka_queue_destroy touches the consumer
      # queue, so scoping the capture to its invocation is both precise and deterministic.
      in_finalizer = false
      destroyed_by_finalizer = []
      closed_by_finalizer = []

      allow(Rdkafka::Bindings).to receive(:rd_kafka_queue_get_consumer).and_wrap_original do |original, *args|
        queue = original.call(*args)
        acquired << queue.address
        queue
      end
      allow(Rdkafka::Bindings).to receive(:rd_kafka_queue_destroy).and_wrap_original do |original, ptr, *rest|
        destroyed_by_finalizer << ptr.address if in_finalizer
        original.call(ptr, *rest)
      end
      allow(Rdkafka::Bindings).to receive(:rd_kafka_consumer_close).and_wrap_original do |original, inner, *rest|
        closed_by_finalizer << inner.address if in_finalizer
        original.call(inner, *rest)
      end

      # Capture the finalizer the consumer registers - this is exactly what GC would invoke.
      registered_finalizer = nil
      allow(ObjectSpace).to receive(:define_finalizer).and_wrap_original do |original, object, *rest, &block|
        registered_finalizer = rest.first || block if object.is_a?(described_class)
        original.call(object, *rest, &block)
      end

      leaky = rdkafka_consumer_config("group.id": SecureRandom.uuid).consumer

      begin
        leaky.subscribe(topic)
        # poll_batch lazily takes the consumer-queue reference via rd_kafka_queue_get_consumer
        leaky.poll_batch(300, max_items: 1)

        expect(acquired.size).to eq(1)
        queue_address = acquired.first
        expect(registered_finalizer).not_to be_nil

        # Run the finalizer exactly as GC would for a consumer collected without an explicit
        # close (GC invokes finalizers with the object id)
        in_finalizer = true
        registered_finalizer.call(leaky.object_id)
        in_finalizer = false

        # The finalizer must destroy the exact pointer poll_batch acquired and close the consumer
        expect(destroyed_by_finalizer).to include(queue_address)
        expect(closed_by_finalizer).not_to be_empty
      ensure
        in_finalizer = false
        leaky.close unless leaky.closed?
      end
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
      expect {
        consumer.poll(100)
      }.to raise_error(Rdkafka::ClosedConsumerError, /poll/)
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

        times.each { |op_time| expect(op_time).to be < close_time }
      end
    end
  end

  describe "#position, #commit, #committed and #store_offset" do
    # Make sure there are messages to work with
    let!(:report) do
      producer.produce(
        topic: topic,
        payload: "payload 1",
        key: "key 1",
        partition: 0
      ).wait
    end

    let(:message) do
      wait_for_message(
        topic: topic,
        delivery_report: report,
        consumer: consumer
      )
    end

    describe "#position" do
      it "onlies accept a topic partition list in position if not nil" do
        expect {
          consumer.position("list")
        }.to raise_error TypeError
      end
    end

    describe "#committed" do
      it "onlies accept a topic partition list in commit if not nil" do
        expect {
          consumer.commit("list")
        }.to raise_error TypeError
      end

      it "commits in sync mode" do
        expect {
          consumer.commit(nil, true)
        }.not_to raise_error
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
        expect(partitions[0].offset).to eq 1
        expect(partitions[1].offset).to eq 2
        expect(partitions[2].offset).to eq 3
      end

      it "raises an error when committing fails" do
        expect(Rdkafka::Bindings).to receive(:rd_kafka_commit).and_return(20)

        expect {
          consumer.commit
        }.to raise_error(Rdkafka::RdkafkaError)
      end

      describe "#committed" do
        it "fetches the committed offsets for the current assignment" do
          partitions = consumer.committed.to_h[topic]
          expect(partitions).not_to be_nil
          expect(partitions[0].offset).to eq 1
        end

        it "fetches the committed offsets for a specified topic partition list" do
          list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
            list.add_topic(topic, [0, 1, 2])
          end
          partitions = consumer.committed(list).to_h[topic]
          expect(partitions).not_to be_nil
          expect(partitions[0].offset).to eq 1
          expect(partitions[1].offset).to eq 1
          expect(partitions[2].offset).to eq 1
        end

        it "raises an error when getting committed fails" do
          expect(Rdkafka::Bindings).to receive(:rd_kafka_committed).and_return(20)
          list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
            list.add_topic(topic, [0, 1, 2])
          end
          expect {
            consumer.committed(list)
          }.to raise_error Rdkafka::RdkafkaError
        end
      end

      describe "#store_offset" do
        let(:consumer) { rdkafka_consumer_config("enable.auto.offset.store": false).consumer }
        let(:metadata) { SecureRandom.uuid }
        let(:group_id) { SecureRandom.uuid }
        let(:base_config) do
          {
            "group.id": group_id,
            "enable.auto.offset.store": false,
            "enable.auto.commit": false
          }
        end

        # Produce a fresh message and consume it with a dedicated consumer
        # to avoid conflicts with the committed offsets from the parent context
        let(:store_offset_report) do
          producer.produce(
            topic: topic,
            payload: "payload store_offset",
            key: "key store_offset",
            partition: 0
          ).wait
        end

        let(:message) do
          wait_for_message(
            topic: topic,
            delivery_report: store_offset_report
          )
        end

        before do
          @new_consumer = rdkafka_consumer_config(base_config).consumer
          @new_consumer.subscribe(topic)
          wait_for_assignment(@new_consumer)
        end

        after do
          @new_consumer.close
        end

        it "stores the offset for a message" do
          @new_consumer.store_offset(message)
          @new_consumer.commit

          # TODO use position here, should be at offset

          list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
            list.add_topic(topic, [0, 1, 2])
          end
          partitions = @new_consumer.committed(list).to_h[topic]
          expect(partitions).not_to be_nil
          expect(partitions[message.partition].offset).to eq(message.offset + 1)
        end

        it "stores the offset for a message with metadata" do
          @new_consumer.store_offset(message, metadata)
          @new_consumer.commit
          @new_consumer.close

          meta_consumer = rdkafka_consumer_config(base_config).consumer
          meta_consumer.subscribe(topic)
          wait_for_assignment(meta_consumer)
          meta_consumer.poll(1_000)
          expect(meta_consumer.committed.to_h[message.topic][message.partition].metadata).to eq(metadata)
          meta_consumer.close
        end

        it "raises an error with invalid input" do
          allow(message).to receive(:partition).and_return(9999)
          expect {
            @new_consumer.store_offset(message)
          }.to raise_error Rdkafka::RdkafkaError
        end

        describe "#position" do
          let(:polled_message) do
            # consumer must poll the message directly (not via a separate consumer)
            # for position to reflect the fetch offset
            report = producer.produce(
              topic: topic,
              payload: "payload position",
              key: "key position",
              partition: 0
            ).wait
            wait_for_message(
              topic: topic,
              delivery_report: report,
              consumer: consumer
            )
          end

          it "fetches the positions for the current assignment" do
            consumer.store_offset(polled_message)

            partitions = consumer.position.to_h[topic]
            expect(partitions).not_to be_nil
            expect(partitions[0].offset).to eq polled_message.offset + 1
          end

          it "fetches the positions for a specified assignment" do
            consumer.store_offset(polled_message)

            list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
              list.add_topic_and_partitions_with_offsets(topic, 0 => nil, 1 => nil, 2 => nil)
            end
            partitions = consumer.position(list).to_h[topic]
            expect(partitions).not_to be_nil
            expect(partitions[0].offset).to eq polled_message.offset + 1
          end

          it "raises an error when getting the position fails" do
            expect(Rdkafka::Bindings).to receive(:rd_kafka_position).and_return(20)

            expect {
              consumer.position
            }.to raise_error(Rdkafka::RdkafkaError)
          end
        end

        context "when trying to use with enable.auto.offset.store set to true" do
          let(:consumer) { rdkafka_consumer_config("enable.auto.offset.store": true).consumer }

          it "expect to raise invalid configuration error" do
            expect { consumer.store_offset(message) }.to raise_error(Rdkafka::RdkafkaError, /invalid_arg/)
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
      expect(low).to eq 0
      expect(high).to be > 0
    end

    it "raises an error when querying offsets fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_query_watermark_offsets).and_return(20)
      expect {
        consumer.query_watermark_offsets(topic, 0, 5000)
      }.to raise_error Rdkafka::RdkafkaError
    end
  end

  describe "#lag" do
    # auto.commit.interval.ms is raised so the default 5s background auto-commit cannot
    # commit our stored offsets before the manual #commit below runs, which would leave
    # nothing to commit and raise no_offset (flaky on overloaded CI).
    let(:consumer) { rdkafka_consumer_config("enable.partition.eof": true, "auto.commit.interval.ms": 60_000).consumer }

    it "calculates the consumer lag" do
      # Make sure there's a message in every partition and
      # wait for the message to make sure everything is committed.
      (0..2).each do |i|
        producer.produce(
          topic: topic,
          key: "key lag #{i}",
          partition: i
        ).wait
      end

      # Consume to the end. We wait for assignment before polling so that
      # partition positions are established — on slow CI (e.g. macOS) the EOF
      # signal can fire before the first fetch completes if we start polling
      # immediately, leaving no stored offsets and causing commit to fail.
      # We also require at least one message to be consumed before stopping,
      # for the same reason.
      consumer.subscribe(topic)
      wait_for_assignment(consumer)
      eof_count = 0
      messages_received = 0
      deadline = Time.now + 30
      loop do
        break if Time.now > deadline
        msg = consumer.poll(100)
        messages_received += 1 if msg
      rescue Rdkafka::RdkafkaError => error
        if error.is_partition_eof?
          eof_count += 1
        end
        break if eof_count >= 3 && messages_received.positive?
      end

      # enable.auto.offset.store is true, but on overloaded CI the offset store lags
      # behind poll. Give it a moment so the offsets are actually stored before
      # committing, otherwise commit raises no_offset.
      sleep(1)

      # Commit
      consumer.commit

      # Create list to fetch lag for. TODO creating the list will not be necessary
      # after committed uses the subscription.
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
      expect(lag).to eq(expected_lag)

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
      expect(lag).to eq(expected_lag)
    end

    it "returns nil if there are no messages on the topic" do
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
      expect(lag).to eq(expected_lag)
    end
  end

  describe "#cluster_id" do
    it "returns the current ClusterId" do
      consumer.subscribe(topic)
      wait_for_assignment(consumer)
      expect(consumer.cluster_id).not_to be_empty
    end

    it "passes the timeout through and frees the native string (no leak)" do
      native_string = FFI::MemoryPointer.from_string("test-cluster-id")
      allow(Rdkafka::Bindings).to receive(:rd_kafka_clusterid).and_return(native_string)
      allow(Rdkafka::Bindings).to receive(:rd_kafka_mem_free)

      expect(consumer.cluster_id(1_234)).to eq("test-cluster-id")
      expect(Rdkafka::Bindings).to have_received(:rd_kafka_clusterid).with(anything, 1_234)
      expect(Rdkafka::Bindings).to have_received(:rd_kafka_mem_free).with(anything, native_string)
    end

    it "returns nil and frees nothing when librdkafka returns NULL" do
      allow(Rdkafka::Bindings).to receive(:rd_kafka_clusterid).and_return(FFI::Pointer::NULL)
      allow(Rdkafka::Bindings).to receive(:rd_kafka_mem_free)

      expect(consumer.cluster_id).to be_nil
      expect(Rdkafka::Bindings).not_to have_received(:rd_kafka_mem_free)
    end
  end

  describe "#member_id" do
    it "returns the current MemberId" do
      consumer.subscribe(topic)
      wait_for_assignment(consumer)
      expect(consumer.member_id).to start_with("rdkafka-")
    end

    it "frees the native string (no leak)" do
      native_string = FFI::MemoryPointer.from_string("rdkafka-member-id")
      allow(Rdkafka::Bindings).to receive(:rd_kafka_memberid).and_return(native_string)
      allow(Rdkafka::Bindings).to receive(:rd_kafka_mem_free)

      expect(consumer.member_id).to eq("rdkafka-member-id")
      expect(Rdkafka::Bindings).to have_received(:rd_kafka_mem_free).with(anything, native_string)
    end

    it "returns nil and frees nothing when librdkafka returns NULL" do
      allow(Rdkafka::Bindings).to receive(:rd_kafka_memberid).and_return(FFI::Pointer::NULL)
      allow(Rdkafka::Bindings).to receive(:rd_kafka_mem_free)

      expect(consumer.member_id).to be_nil
      expect(Rdkafka::Bindings).not_to have_received(:rd_kafka_mem_free)
    end
  end

  describe "#metadata" do
    it "returns metadata for all topics when no topic name is given" do
      # Force topic creation before querying metadata
      topic
      result = consumer.metadata.topics.map { |t| t[:topic_name] }
      expect(result).to include(topic)
    end

    it "returns metadata for the given topic" do
      expect(consumer.metadata(topic).topics.first[:topic_name]).to eq(topic)
    end
  end

  describe "#poll" do
    it "returns nil if there is no subscription" do
      expect(consumer.poll(1000)).to be_nil
    end

    it "returns nil if there are no messages" do
      consumer.subscribe(topic)
      expect(consumer.poll(1000)).to be_nil
    end

    it "returns a message if there is one" do
      topic = TestTopics.create

      producer.produce(
        topic: topic,
        payload: "payload 1",
        key: "key 1"
      ).wait
      consumer.subscribe(topic)
      message = consumer.each { |m| break m }

      expect(message).to be_a Rdkafka::Consumer::Message
      expect(message.payload).to eq("payload 1")
      expect(message.key).to eq("key 1")
    end

    it "raises an error when polling fails" do
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

    it "expect to raise error when polling non-existing topic" do
      missing_topic = SecureRandom.uuid
      consumer.subscribe(missing_topic)

      expect {
        5.times { consumer.poll(1_000) }
      }.to raise_error Rdkafka::RdkafkaError, /Subscribed topic not available: #{missing_topic}/
    end
  end

  describe "#poll_nb" do
    it "returns nil if there is no subscription" do
      expect(consumer.poll_nb).to be_nil
    end

    it "returns nil if there are no messages" do
      consumer.subscribe(topic)
      expect(consumer.poll_nb).to be_nil
    end

    it "accepts a timeout parameter" do
      consumer.subscribe(topic)
      expect(consumer.poll_nb(0)).to be_nil
      expect(consumer.poll_nb(100)).to be_nil
    end

    it "returns a message if there is one" do
      topic = TestTopics.create

      producer.produce(
        topic: topic,
        payload: "payload poll_nb",
        key: "key poll_nb"
      ).wait

      consumer.subscribe(topic)
      wait_for_assignment(consumer)

      # Give time for message to arrive
      sleep 1

      message = nil
      10.times do
        message = consumer.poll_nb(100)
        break if message
        sleep 0.1
      end

      expect(message).to be_a Rdkafka::Consumer::Message
      expect(message.payload).to eq("payload poll_nb")
      expect(message.key).to eq("key poll_nb")
    end

    it "raises an error when polling fails" do
      message = Rdkafka::Bindings::Message.new.tap do |message|
        message[:err] = 20
      end
      message_pointer = message.to_ptr
      expect(Rdkafka::Bindings).to receive(:rd_kafka_consumer_poll_nb).and_return(message_pointer)
      expect(Rdkafka::Bindings).to receive(:rd_kafka_message_destroy).with(message_pointer)
      expect {
        consumer.poll_nb
      }.to raise_error Rdkafka::RdkafkaError
    end

    context "when consumer is closed" do
      before { consumer.close }

      it "raises ClosedConsumerError" do
        expect { consumer.poll_nb }.to raise_error(Rdkafka::ClosedConsumerError, /poll_nb/)
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
      expect(message).to be
      expect(message.key).to eq("key headers")
      expect(message.headers).to include("foo" => "bar")
    end

    it "returns message with headers using string keys (when produced with string keys)" do
      report = producer.produce(
        topic: topic,
        key: "key headers",
        headers: { "foo" => "bar" }
      ).wait

      message = wait_for_message(topic: topic, consumer: consumer, delivery_report: report)
      expect(message).to be
      expect(message.key).to eq("key headers")
      expect(message.headers).to include("foo" => "bar")
    end

    it "returns message with no headers" do
      report = producer.produce(
        topic: topic,
        key: "key no headers",
        headers: nil
      ).wait

      message = wait_for_message(topic: topic, consumer: consumer, delivery_report: report)
      expect(message).to be
      expect(message.key).to eq("key no headers")
      expect(message.headers).to be_empty
    end

    it "raises an error when message headers aren't readable" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_message_headers).with(any_args).and_return(1)

      report = producer.produce(
        topic: topic,
        key: "key err headers",
        headers: nil
      ).wait

      expect {
        wait_for_message(topic: topic, consumer: consumer, delivery_report: report)
      }.to raise_error do |err|
        expect(err).to be_instance_of(Rdkafka::RdkafkaError)
        expect(err.message).to start_with("Error reading message headers")
      end
    end

    it "raises an error when the first message header aren't readable" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_header_get_all).with(any_args).and_return(1)

      report = producer.produce(
        topic: topic,
        key: "key err headers",
        headers: { foo: "bar" }
      ).wait

      expect {
        wait_for_message(topic: topic, consumer: consumer, delivery_report: report)
      }.to raise_error do |err|
        expect(err).to be_instance_of(Rdkafka::RdkafkaError)
        expect(err.message).to start_with("Error reading a message header at index 0")
      end
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
        expect(message).to be_a Rdkafka::Consumer::Message
        break if i == 9
      end
      consumer.close
    end
  end

  describe "#each_batch" do
    it "expect to raise an error" do
      expect do
        consumer.each_batch {}
      end.to raise_error(NotImplementedError)
    end
  end

  describe "#offsets_for_times" do
    it "raises when not TopicPartitionList" do
      expect { consumer.offsets_for_times([]) }.to raise_error(TypeError)
    end

    it "raises an error when offsets_for_times fails" do
      tpl = Rdkafka::Consumer::TopicPartitionList.new

      expect(Rdkafka::Bindings).to receive(:rd_kafka_offsets_for_times).and_return(7)

      expect { consumer.offsets_for_times(tpl) }.to raise_error(Rdkafka::RdkafkaError)
    end

    context "when subscribed" do
      let(:timeout) { 1000 }

      before do
        consumer.subscribe(topic)

        # 1. partitions are assigned
        wait_for_assignment(consumer)
        expect(consumer.assignment).not_to be_empty

        # 2. eat unrelated messages
        while consumer.poll(timeout) do; end
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

        consumer.poll(timeout)
        message = consumer.poll(timeout)
        consumer.poll(timeout)

        tpl = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
          list.add_topic_and_partitions_with_offsets(
            topic,
            [
              [0, message.timestamp]
            ]
          )
        end

        tpl_response = consumer.offsets_for_times(tpl)

        expect(tpl_response.to_h[topic][0].offset).to eq message.offset
      end
    end
  end

  # Only relevant in case of a consumer with separate queues
  describe "#events_poll" do
    let(:stats) { [] }
    let(:consumer) do
      config = rdkafka_consumer_config("statistics.interval.ms": 500)
      config.consumer_poll_set = false
      config.consumer
    end

    before do
      # Force topic creation before setting the statistics callback so the admin
      # client used inside TestTopics.create closes without the StatsCallback
      # competing for the GVL (which can hang the admin's polling thread join).
      topic
      Rdkafka::Config.statistics_callback = ->(published) { stats << published }
    end

    after { Rdkafka::Config.statistics_callback = nil }

    it "expect to run events_poll, operate and propagate stats on events_poll and not poll" do
      consumer.subscribe(topic)
      consumer.poll(1_000)
      expect(stats).to be_empty
      consumer.events_poll(-1)
      expect(stats).not_to be_empty
    end
  end

  # Only relevant in case of a consumer with separate queues
  describe "#events_poll_nb" do
    let(:stats) { [] }
    let(:consumer) do
      config = rdkafka_consumer_config("statistics.interval.ms": 500)
      config.consumer_poll_set = false
      config.consumer
    end

    before do
      # Force topic creation before setting the statistics callback so the admin
      # client used inside TestTopics.create closes without the StatsCallback
      # competing for the GVL (which can hang the admin's polling thread join).
      topic
      Rdkafka::Config.statistics_callback = ->(published) { stats << published }
    end

    after { Rdkafka::Config.statistics_callback = nil }

    it "returns the number of events processed" do
      consumer.subscribe(topic)
      result = consumer.events_poll_nb
      expect(result).to be_a(Integer)
      expect(result).to be >= 0
    end

    it "accepts a timeout parameter" do
      consumer.subscribe(topic)
      expect(consumer.events_poll_nb(0)).to be >= 0
      expect(consumer.events_poll_nb(100)).to be >= 0
    end

    it "processes events without releasing GVL" do
      consumer.subscribe(topic)
      consumer.poll(1_000)
      expect(stats).to be_empty

      # Wait for statistics to be ready
      sleep 0.6

      # Non-blocking poll should also process stats events
      consumer.events_poll_nb(100)
      expect(stats).not_to be_empty
    end
  end

  describe "#consumer_group_metadata_pointer" do
    let(:pointer) { consumer.consumer_group_metadata_pointer }

    after { Rdkafka::Bindings.rd_kafka_consumer_group_metadata_destroy(pointer) }

    it "expect to return a pointer" do
      expect(pointer).to be_a(FFI::Pointer)
    end
  end

  describe "a rebalance listener" do
    let(:consumer) do
      config = rdkafka_consumer_config
      config.consumer_rebalance_listener = listener
      config.consumer
    end

    context "with a working listener" do
      let(:listener) do
        Struct.new(:queue) do
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
      end

      it "gets notifications" do
        notify_listener(listener, topic: topic)

        expect(listener.queue).to eq([
          [:assign, topic, 0, 1, 2],
          [:revoke, topic, 0, 1, 2]
        ])
      end
    end

    context "with a broken listener" do
      let(:listener) do
        Struct.new(:queue) do
          def on_partitions_assigned(list)
            queue << :assigned
            raise "boom"
          end

          def on_partitions_revoked(list)
            queue << :revoked
            raise "boom"
          end
        end.new([])
      end

      it "handles callback exceptions" do
        notify_listener(listener, topic: topic)

        expect(listener.queue).to eq([:assigned, :revoked])
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
      position: [],
      query_watermark_offsets: [nil, nil],
      assignment_lost?: [],
      poll_nb: [],
      metadata: [nil]
    }.each do |method, args|
      it "raises an exception if #{method} is called" do
        expect {
          if args.nil?
            consumer.public_send(method)
          else
            consumer.public_send(method, *args)
          end
        }.to raise_exception(Rdkafka::ClosedConsumerError, /#{method}/)
      end
    end
  end

  context "when the rebalance protocol is cooperative" do
    let(:consumer) do
      config = rdkafka_consumer_config(
        {
          "partition.assignment.strategy": "cooperative-sticky",
          debug: "consumer"
        }
      )
      config.consumer_rebalance_listener = listener
      config.consumer
    end

    let(:listener) do
      Struct.new(:queue) do
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
    end

    it "is able to assign and unassign partitions using the cooperative partition assignment APIs" do
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
          expect(message).to be_a Rdkafka::Consumer::Message
          break if i == 9
        end
      end

      expect(listener.queue).to eq([
        [:assign, topic, 0, 1, 2],
        [:revoke, topic, 0, 1, 2]
      ])
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
        expect(response).to eq(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE)
      end
    end

    context "when sasl configured" do
      before do
        $consumer_sasl = rdkafka_producer_config(
          "security.protocol": "sasl_ssl",
          "sasl.mechanisms": "OAUTHBEARER"
        ).consumer
      end

      after do
        $consumer_sasl.close
      end

      context "without extensions" do
        it "succeeds" do
          response = $consumer_sasl.oauthbearer_set_token(
            token: "foo",
            lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
            principal_name: "kafka-cluster"
          )
          expect(response).to eq(0)
        end
      end

      context "with extensions" do
        it "succeeds" do
          response = $consumer_sasl.oauthbearer_set_token(
            token: "foo",
            lifetime_ms: Time.now.to_i * 1000 + 900 * 1000,
            principal_name: "kafka-cluster",
            extensions: {
              "foo" => "bar"
            }
          )
          expect(response).to eq(0)
        end
      end
    end
  end

  describe "when reaching eof on a topic and eof reporting enabled" do
    let(:consumer) { rdkafka_consumer_config("enable.partition.eof": true).consumer }

    def collect_eof_error(consumer, &poll_block)
      consumer.subscribe(topic)
      eof_error = nil
      deadline = Time.now + 30

      loop do
        break if Time.now > deadline
        poll_block.call
      rescue Rdkafka::RdkafkaError => error
        if error.is_partition_eof?
          eof_error = error
          break
        end
      end

      eof_error
    end

    def expect_eof_details(error, expected_topic)
      expect(error).not_to be_nil
      expect(error.code).to eq(:partition_eof)
      expect(error.details[:topic]).to eq(expected_topic)
      expect(error.details[:partition]).to be_a(Integer)
      expect(error.details[:offset]).to be_a(Integer)
    end

    before do
      producer.produce(topic: topic, key: "key eof", partition: 0).wait
    end

    it "raises :partition_eof with topic/partition/offset details via #poll" do
      error = collect_eof_error(consumer) { consumer.poll(100) }
      expect_eof_details(error, topic)
    end

    it "raises :partition_eof with topic/partition/offset details via #poll_nb" do
      error = collect_eof_error(consumer) { consumer.poll_nb(100) }
      expect_eof_details(error, topic)
    end

    it "raises :partition_eof with topic/partition/offset details via #poll_nb_each" do
      error = collect_eof_error(consumer) do
        consumer.poll_nb_each { |_| }
        sleep 0.05
      end
      expect_eof_details(error, topic)
    end

    it "returns :partition_eof with topic/partition/offset details via #poll_batch" do
      consumer.subscribe(topic)
      eof_error = nil
      deadline = Time.now + 30
      loop do
        break if Time.now > deadline
        results = consumer.poll_batch(100, max_items: 10)
        eof_error = results.find { |r| r.is_a?(Rdkafka::RdkafkaError) && r.is_partition_eof? }
        break if eof_error
      end
      expect_eof_details(eof_error, topic)
    end

    it "returns :partition_eof with topic/partition/offset details via #poll_batch_nb" do
      consumer.subscribe(topic)
      eof_error = nil
      deadline = Time.now + 30
      loop do
        break if Time.now > deadline
        results = consumer.poll_batch_nb(100, max_items: 10)
        eof_error = results.find { |r| r.is_a?(Rdkafka::RdkafkaError) && r.is_partition_eof? }
        break if eof_error
      end
      expect_eof_details(eof_error, topic)
    end
  end

  describe "long running consumption" do
    let(:consumer) { rdkafka_consumer_config.consumer }
    let(:producer) { rdkafka_producer_config.producer }

    after {
      consumer.close
      producer.close
    }

    it "consumes messages continuously for 60 seconds" do
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
          expect(message).to be_a Rdkafka::Consumer::Message
          expect(message.topic).to eq(topic)
          messages_consumed += 1
          consumer.commit if messages_consumed % 10 == 0
        end
      end

      producer_thread.join

      expect(messages_consumed).to be > 50 # Should consume most messages
    end
  end

  describe "#events_poll_nb_each" do
    it "does not raise when queue is empty" do
      expect { consumer.events_poll_nb_each { |_| } }.not_to raise_error
    end

    it "yields the count after each poll" do
      counts = []
      # Stub to return events, then zero
      call_count = 0
      allow(Rdkafka::Bindings).to receive(:rd_kafka_poll_nb) do
        call_count += 1
        (call_count <= 2) ? 1 : 0
      end

      consumer.events_poll_nb_each { |count| counts << count }

      expect(counts).to eq([1, 1])
    end

    it "stops when block returns :stop" do
      iterations = 0
      # Stub to always return events
      allow(Rdkafka::Bindings).to receive(:rd_kafka_poll_nb).and_return(1)

      consumer.events_poll_nb_each do |_count|
        iterations += 1
        :stop if iterations >= 3
      end

      expect(iterations).to eq(3)
    end

    context "when consumer is closed" do
      before { consumer.close }

      it "raises ClosedConsumerError" do
        expect { consumer.events_poll_nb_each { |_| } }.to raise_error(Rdkafka::ClosedConsumerError, /events_poll_nb_each/)
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
      expect(messages).to be_a(Array)
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

      expect(messages.size).to eq(1)
    end

    it "properly cleans up message pointers" do
      consumer.subscribe(topic)

      producer.produce(topic: topic, payload: "cleanup test")
      producer.flush
      sleep 2

      # This should not leak memory - message_destroy is called in ensure
      expect {
        consumer.poll_nb_each { |_| }
      }.not_to raise_error
    end

    context "when consumer is closed" do
      before { consumer.close }

      it "raises ClosedConsumerError" do
        expect { consumer.poll_nb_each { |_| } }.to raise_error(Rdkafka::ClosedConsumerError, /poll_nb_each/)
      end
    end
  end

  describe "#poll_batch" do
    it "returns empty array if there is no subscription" do
      expect(consumer.poll_batch(1000, max_items: 10)).to eq([])
    end

    it "returns empty array if there are no messages" do
      consumer.subscribe(topic)
      expect(consumer.poll_batch(1000, max_items: 10)).to eq([])
    end

    it "returns messages if there are some" do
      topic = TestTopics.create

      3.times do |i|
        producer.produce(
          topic: topic,
          payload: "payload #{i}",
          key: "key #{i}"
        ).wait
      end

      consumer.subscribe(topic)

      messages = []
      deadline = Time.now + 30
      while messages.size < 3 && Time.now < deadline
        messages.concat(consumer.poll_batch(1000, max_items: 10))
      end

      expect(messages.size).to eq(3)
      messages.each do |message|
        expect(message).to be_a(Rdkafka::Consumer::Message)
      end
      expect(messages.map(&:payload)).to contain_exactly("payload 0", "payload 1", "payload 2")
    end

    it "surfaces a per-message build error inline and keeps the rest of the batch" do
      topic = TestTopics.create

      3.times do |i|
        producer.produce(topic: topic, payload: "payload #{i}", key: "key #{i}", partition: 0).wait
      end

      # Fail building exactly one message (a header read error). Before the fix this raised out of
      # poll_batch and discarded every message already built; now it is returned inline.
      call = 0
      allow(Rdkafka::Consumer::Headers).to receive(:from_native).and_wrap_original do |original, *args|
        call += 1
        raise Rdkafka::RdkafkaError.new(-185, "Error reading message headers") if call == 2

        original.call(*args)
      end

      consumer.subscribe(topic)

      results = []
      deadline = Time.now + 30
      while results.size < 3 && Time.now < deadline
        results.concat(consumer.poll_batch(1000, max_items: 10))
      end

      built = results.select { |r| r.is_a?(Rdkafka::Consumer::Message) }
      errors = results.select { |r| r.is_a?(Rdkafka::RdkafkaError) }

      expect(results.size).to eq(3)
      expect(built.size).to eq(2)
      expect(errors.size).to eq(1)
    end

    it "respects max_items" do
      topic = TestTopics.create

      10.times do |i|
        producer.produce(
          topic: topic,
          payload: "payload #{i}",
          key: "key #{i}"
        ).wait
      end

      consumer.subscribe(topic)

      # Wait for messages to be fetched
      deadline = Time.now + 30
      first = nil
      while first.nil? && Time.now < deadline
        first = consumer.poll(1000)
      end
      expect(first).not_to be_nil

      sleep 1

      batch = consumer.poll_batch(1000, max_items: 3)
      expect(batch.size).to be <= 3
    end

    it "returns error events inline rather than raising" do
      message = Rdkafka::Bindings::Message.new.tap { |msg| msg[:err] = 20 }
      message_pointer = message.to_ptr
      buffer = FFI::MemoryPointer.new(:pointer, 1)
      buffer.put_pointer(0, message_pointer)

      expect(Rdkafka::Bindings).to receive(:rd_kafka_consume_batch_queue).and_return(1)
      allow(consumer).to receive_messages(batch_buffer: buffer, consumer_queue: FFI::Pointer::NULL)
      expect(Rdkafka::Bindings).to receive(:rd_kafka_message_destroy).with(message_pointer)

      consumer.subscribe(topic)

      results = consumer.poll_batch(100, max_items: 1)
      expect(results.size).to eq(1)
      expect(results.first).to be_a(Rdkafka::RdkafkaError)
      expect(results.first.rdkafka_response).to eq(20)
    end

    it "returns multiple error events from a batch in order" do
      msg1 = Rdkafka::Bindings::Message.new.tap { |m| m[:err] = 20 }
      msg2 = Rdkafka::Bindings::Message.new.tap { |m| m[:err] = 10 }
      ptr1 = msg1.to_ptr
      ptr2 = msg2.to_ptr

      buffer = FFI::MemoryPointer.new(:pointer, 2)
      buffer.put_pointer(0, ptr1)
      buffer.put_pointer(FFI::Pointer.size, ptr2)

      expect(Rdkafka::Bindings).to receive(:rd_kafka_consume_batch_queue).and_return(2)
      allow(consumer).to receive_messages(batch_buffer: buffer, consumer_queue: FFI::Pointer::NULL)
      expect(Rdkafka::Bindings).to receive(:rd_kafka_message_destroy).with(ptr1)
      expect(Rdkafka::Bindings).to receive(:rd_kafka_message_destroy).with(ptr2)

      consumer.subscribe(topic)

      results = consumer.poll_batch(100, max_items: 2)
      expect(results.size).to eq(2)
      expect(results).to all(be_a(Rdkafka::RdkafkaError))
      expect(results.map(&:rdkafka_response)).to eq([20, 10])
    end

    it "returns messages and errors interleaved in arrival order" do
      err_msg = Rdkafka::Bindings::Message.new.tap { |m| m[:err] = 20 }
      ok_msg = Rdkafka::Bindings::Message.new.tap { |m| m[:err] = 0 }
      ptr_err = err_msg.to_ptr
      ptr_ok = ok_msg.to_ptr

      buffer = FFI::MemoryPointer.new(:pointer, 2)
      buffer.put_pointer(0, ptr_err)
      buffer.put_pointer(FFI::Pointer.size, ptr_ok)

      fake_message = double("message")
      expect(Rdkafka::Bindings).to receive(:rd_kafka_consume_batch_queue).and_return(2)
      allow(consumer).to receive_messages(batch_buffer: buffer, consumer_queue: FFI::Pointer::NULL)
      allow(Rdkafka::Consumer::Message).to receive(:new).and_return(fake_message)
      expect(Rdkafka::Bindings).to receive(:rd_kafka_message_destroy).with(ptr_err)
      expect(Rdkafka::Bindings).to receive(:rd_kafka_message_destroy).with(ptr_ok)

      consumer.subscribe(topic)

      results = consumer.poll_batch(100, max_items: 2)
      expect(results.size).to eq(2)
      expect(results[0]).to be_a(Rdkafka::RdkafkaError)
      expect(results[1]).to eq(fake_message)
    end

    context "when consumer is closed" do
      before { consumer.close }

      it "raises ClosedConsumerError" do
        expect {
          consumer.poll_batch(100, max_items: 10)
        }.to raise_error(Rdkafka::ClosedConsumerError, /poll_batch/)
      end
    end
  end

  describe "#poll_batch_nb" do
    it "returns empty array if there is no subscription" do
      expect(consumer.poll_batch_nb(0, max_items: 10)).to eq([])
    end

    it "returns empty array if there are no messages" do
      consumer.subscribe(topic)
      sleep 0.5
      expect(consumer.poll_batch_nb(0, max_items: 10)).to eq([])
    end

    it "defaults to timeout 0" do
      consumer.subscribe(topic)
      sleep 0.5
      expect(consumer.poll_batch_nb(max_items: 10)).to eq([])
    end

    it "returns messages if there are some" do
      topic = TestTopics.create

      3.times do |i|
        producer.produce(
          topic: topic,
          payload: "nb payload #{i}",
          key: "nb key #{i}"
        ).wait
      end

      consumer.subscribe(topic)

      # Wait for messages to arrive in the internal queue
      deadline = Time.now + 30
      first = nil
      while first.nil? && Time.now < deadline
        first = consumer.poll(1000)
      end
      expect(first).not_to be_nil

      sleep 1

      messages = consumer.poll_batch_nb(0, max_items: 10)
      messages.each do |message|
        expect(message).to be_a(Rdkafka::Consumer::Message)
      end
    end

    it "returns error events inline rather than raising" do
      message = Rdkafka::Bindings::Message.new.tap { |msg| msg[:err] = 20 }
      message_pointer = message.to_ptr
      buffer = FFI::MemoryPointer.new(:pointer, 1)
      buffer.put_pointer(0, message_pointer)

      expect(Rdkafka::Bindings).to receive(:rd_kafka_consume_batch_queue_nb).and_return(1)
      allow(consumer).to receive_messages(batch_buffer: buffer, consumer_queue: FFI::Pointer::NULL)
      expect(Rdkafka::Bindings).to receive(:rd_kafka_message_destroy).with(message_pointer)

      consumer.subscribe(topic)

      results = consumer.poll_batch_nb(0, max_items: 1)
      expect(results.size).to eq(1)
      expect(results.first).to be_a(Rdkafka::RdkafkaError)
      expect(results.first.rdkafka_response).to eq(20)
    end

    it "returns multiple error events from a batch in order" do
      msg1 = Rdkafka::Bindings::Message.new.tap { |m| m[:err] = 20 }
      msg2 = Rdkafka::Bindings::Message.new.tap { |m| m[:err] = 10 }
      ptr1 = msg1.to_ptr
      ptr2 = msg2.to_ptr

      buffer = FFI::MemoryPointer.new(:pointer, 2)
      buffer.put_pointer(0, ptr1)
      buffer.put_pointer(FFI::Pointer.size, ptr2)

      expect(Rdkafka::Bindings).to receive(:rd_kafka_consume_batch_queue_nb).and_return(2)
      allow(consumer).to receive_messages(batch_buffer: buffer, consumer_queue: FFI::Pointer::NULL)
      expect(Rdkafka::Bindings).to receive(:rd_kafka_message_destroy).with(ptr1)
      expect(Rdkafka::Bindings).to receive(:rd_kafka_message_destroy).with(ptr2)

      consumer.subscribe(topic)

      results = consumer.poll_batch_nb(0, max_items: 2)
      expect(results.size).to eq(2)
      expect(results).to all(be_a(Rdkafka::RdkafkaError))
      expect(results.map(&:rdkafka_response)).to eq([20, 10])
    end

    it "returns messages and errors interleaved in arrival order" do
      err_msg = Rdkafka::Bindings::Message.new.tap { |m| m[:err] = 20 }
      ok_msg = Rdkafka::Bindings::Message.new.tap { |m| m[:err] = 0 }
      ptr_err = err_msg.to_ptr
      ptr_ok = ok_msg.to_ptr

      buffer = FFI::MemoryPointer.new(:pointer, 2)
      buffer.put_pointer(0, ptr_err)
      buffer.put_pointer(FFI::Pointer.size, ptr_ok)

      fake_message = double("message")
      expect(Rdkafka::Bindings).to receive(:rd_kafka_consume_batch_queue_nb).and_return(2)
      allow(consumer).to receive_messages(batch_buffer: buffer, consumer_queue: FFI::Pointer::NULL)
      allow(Rdkafka::Consumer::Message).to receive(:new).and_return(fake_message)
      expect(Rdkafka::Bindings).to receive(:rd_kafka_message_destroy).with(ptr_err)
      expect(Rdkafka::Bindings).to receive(:rd_kafka_message_destroy).with(ptr_ok)

      consumer.subscribe(topic)

      results = consumer.poll_batch_nb(0, max_items: 2)
      expect(results.size).to eq(2)
      expect(results[0]).to be_a(Rdkafka::RdkafkaError)
      expect(results[1]).to eq(fake_message)
    end

    context "when consumer is closed" do
      before { consumer.close }

      it "raises ClosedConsumerError" do
        expect {
          consumer.poll_batch_nb(0, max_items: 10)
        }.to raise_error(Rdkafka::ClosedConsumerError, /poll_batch_nb/)
      end
    end
  end

  describe "file descriptor access for fiber scheduler integration" do
    it "enables IO events on consumer queue" do
      consumer.subscribe(topic)

      signal_r, signal_w = IO.pipe
      expect { consumer.enable_queue_io_events(signal_w.fileno) }.not_to raise_error
      signal_r.close
      signal_w.close
    end

    it "enables IO events on background queue" do
      consumer.subscribe(topic)

      signal_r, signal_w = IO.pipe
      expect { consumer.enable_background_queue_io_events(signal_w.fileno) }.not_to raise_error
      signal_r.close
      signal_w.close
    end

    it "enables FD with payload option" do
      consumer.subscribe(topic)

      signal_r, signal_w = IO.pipe
      custom_payload = "hello"
      expect { consumer.enable_queue_io_events(signal_w.fileno, custom_payload) }.not_to raise_error
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
      expect(messages).to be_a(Array)
      signal_r.close
      signal_w.close
    end

    context "when consumer is closed" do
      before { consumer.close }

      it "raises ClosedInnerError when enabling queue_io_events" do
        signal_r, signal_w = IO.pipe
        expect { consumer.enable_queue_io_events(signal_w.fileno) }.to raise_error(Rdkafka::ClosedInnerError)
        signal_r.close
        signal_w.close
      end

      it "raises ClosedInnerError when enabling background_queue_io_events" do
        signal_r, signal_w = IO.pipe
        expect { consumer.enable_background_queue_io_events(signal_w.fileno) }.to raise_error(Rdkafka::ClosedInnerError)
        signal_r.close
        signal_w.close
      end
    end
  end
end
