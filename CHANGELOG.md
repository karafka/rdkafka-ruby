# 0.11.0
* Upgrade librdkafka to 1.8.2
* Bump supported minimum Ruby version to 2.6
* Better homebrew path detection

# 0.10.0
* Upgrade librdkafka to 1.5.0
* Add error callback config

# 0.9.0
* Fixes for Ruby 3.0
* Allow any callable object for callbacks (gremerritt)
* Reduce memory allocations in Rdkafka::Producer#produce (jturkel)
* Use queue as log callback to avoid unsafe calls from trap context (breunigs)
* Allow passing in topic configuration on create_topic (dezka)
* Add each_batch method to consumer (mgrosso)

# 0.8.1
* Fix topic_flag behaviour and add tests for Metadata (geoff2k)
* Add topic admin interface (geoff2k)
* Raise an exception if @native_kafka is nil (geoff2k)
* Option to use zstd compression (jasonmartens)

# 0.8.0
* Upgrade librdkafka to 1.4.0
* Integrate librdkafka metadata API and add partition_key (by Adithya-copart)
* Ruby 2.7 compatibility fix (by Geoff Thé)A
* Add error to delivery report (by Alex Stanovsky)
* Don't override CPPFLAGS and LDFLAGS if already set on Mac (by Hiroshi Hatake)
* Allow use of Rake 13.x and up (by Tomasz Pajor)

# 0.7.0
* Bump librdkafka to 1.2.0 (by rob-as)
* Allow customizing the wait time for delivery report availability (by mensfeld)

# 0.6.0
* Bump librdkafka to 1.1.0 (by Chris Gaffney)
* Implement seek (by breunigs)

# 0.5.0
* Bump librdkafka to 1.0.0 (by breunigs)
* Add cluster and member information (by dmexe)
* Support message headers for consumer & producer (by dmexe)
* Add consumer rebalance listener (by dmexe)
* Implement pause/resume partitions (by dmexe)

# 0.4.2
* Delivery callback for producer
* Document list param of commit method
* Use default Homebrew openssl location if present
* Consumer lag handles empty topics
* End iteration in consumer when it is closed
* Add support for storing message offsets
* Add missing runtime dependency to rake

# 0.4.1
* Bump librdkafka to 0.11.6

# 0.4.0
* Improvements in librdkafka archive download
* Add global statistics callback
* Use Time for timestamps, potentially breaking change if you
  rely on the previous behavior where it returns an integer with
  the number of milliseconds.
* Bump librdkafka to 0.11.5
* Implement TopicPartitionList in Ruby so we don't have to keep
  track of native objects.
* Support committing a topic partition list
* Add consumer assignment method

# 0.3.5
* Fix crash when not waiting for delivery handles
* Run specs on Ruby 2.5

# 0.3.4
* Bump librdkafka to 0.11.3

# 0.3.3
* Fix bug that prevent display of `RdkafkaError` message

# 0.3.2
* `add_topic` now supports using a partition count
* Add way to make errors clearer with an extra message
* Show topics in subscribe error message
* Show partition and topic in query watermark offsets error message

# 0.3.1
* Bump librdkafka to 0.11.1
* Officially support ranges in `add_topic` for topic partition list.
* Add consumer lag calculator

# 0.3.0
* Move both add topic methods to one `add_topic` in `TopicPartitionList`
* Add committed offsets to consumer
* Add query watermark offset to consumer

# 0.2.0
* Some refactoring and add inline documentation

# 0.1.x
* Initial working version including producing and consuming
