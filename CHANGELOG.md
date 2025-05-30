# Rdkafka Changelog

## 0.19.5 (2025-05-30)
- [Enhancement] Allow for producing to non-existing topics with `key` and `partition_key` present.

## 0.19.4 (2025-05-23)
- [Change] Move to trusted-publishers and remove signing since no longer needed.

## 0.19.3 (2025-05-23)
- [Enhancement] Include broker message in the error full message if provided.

## 0.19.2 (2025-05-20)
- [Enhancement] Replace TTL-based partition count cache with a global cache that reuses `librdkafka` statistics data when possible.
- [Enhancement] Roll out experimental jruby support.
- [Fix] Fix issue where post-closed producer C topics refs would not be cleaned.
- [Fix] Fiber causes Segmentation Fault.
- [Change] Move to trusted-publishers and remove signing since no longer needed.

## 0.19.1 (2025-04-07)
- [Enhancement] Support producing and consuming of headers with mulitple values (KIP-82).
- [Enhancement] Allow native Kafka customization poll time.

## 0.19.0 (2025-01-20)
- **[Breaking]** Deprecate and remove `#each_batch` due to data consistency concerns.
- [Enhancement] Bump librdkafka to 2.8.0
- [Fix] Restore `Rdkafka::Bindings.rd_kafka_global_init` as it was not the source of the original issue.

## 0.18.1 (2024-12-04)
- [Fix] Do not run `Rdkafka::Bindings.rd_kafka_global_init` on require to prevent some of macos versions from hanging on Puma fork.

## 0.18.0 (2024-11-26)
- **[Breaking]** Drop Ruby 3.0 support
- [Enhancement] Bump librdkafka to 2.6.1
- [Enhancement] Use default oauth callback if none is passed (bachmanity1)
- [Enhancement] Expose `rd_kafka_global_init` to mitigate macos forking issues.
- [Patch] Retire no longer needed cooperative-sticky patch.

## 0.17.6 (2024-09-03)
- [Fix] Fix incorrectly behaving CI on failures. 
- [Fix] Fix invalid patches librdkafka references.

## 0.17.5 (2024-09-03)
- [Patch] Patch with "Add forward declaration to fix compilation without ssl" fix

## 0.17.4 (2024-09-02)
- [Enhancement] Bump librdkafka to 2.5.3
- [Enhancement] Do not release GVL on `rd_kafka_name` (ferrous26)
- [Fix] Fix unused variable reference in producer (lucasmvnascimento)

## 0.17.3 (2024-08-09)
- [Fix] Mitigate a case where FFI would not restart the background events callback dispatcher in forks.

## 0.17.2 (2024-08-07)
- [Enhancement] Support returning `#details` for errors that do have topic/partition related extra info.

## 0.17.1 (2024-08-01)
- [Enhancement] Support ability to release patches to librdkafka.
- [Patch] Patch cooperative-sticky assignments in librdkafka.

## 0.17.0 (2024-07-21)
- [Enhancement] Bump librdkafka to 2.5.0

## 0.16.1 (2024-07-10)
- [Feature] Add `#seek_by` to be able to seek for a message by topic, partition and offset (zinahia)
- [Change] Remove old producer timeout API warnings.
- [Fix] Switch to local release of librdkafka to mitigate its unavailability.

## 0.16.0 (2024-06-17)
- **[Breaking]** Messages without headers returned by `#poll` contain frozen empty hash.
- **[Breaking]** `HashWithSymbolKeysTreatedLikeStrings` has been removed so headers are regular hashes with string keys.
- [Enhancement] Bump librdkafka to 2.4.0
- [Enhancement] Save two objects on message produced and lower CPU usage on message produced with small improvements.
- [Fix] Remove support for Ruby 2.7. Supporting it was a bug since rest of the karafka ecosystem no longer supports it.

## 0.15.2 (2024-07-10)
- [Fix] Switch to local release of librdkafka to mitigate its unavailability.

## 0.15.1 (2024-05-09)
- **[Feature]** Provide ability to use topic config on a producer for custom behaviors per dispatch.
- [Enhancement] Use topic config reference cache for messages production to prevent topic objects allocation with each message.
- [Enhancement] Provide `Rrdkafka::Admin#describe_errors` to get errors descriptions (mensfeld)

## 0.15.0 (2024-04-26)
- **[Feature]** Oauthbearer token refresh callback (bruce-szalwinski-he)
- **[Feature]** Support incremental config describe + alter API (mensfeld)
- [Enhancement] name polling Thread as `rdkafka.native_kafka#<name>` (nijikon)
- [Enhancement] Replace time poll based wait engine with an event based to improve response times on blocking operations and wait (nijikon + mensfeld)
- [Enhancement] Allow for usage of the second regex engine of librdkafka by setting `RDKAFKA_DISABLE_REGEX_EXT` during build (mensfeld)
- [Enhancement] name polling Thread as `rdkafka.native_kafka#<name>` (nijikon)
- [Change] Allow for native kafka thread operations deferring and manual start for consumer, producer and admin.
- [Change] The `wait_timeout` argument in `AbstractHandle.wait` method is deprecated and will be removed in future versions without replacement. We don't rely on it's value anymore (nijikon)
- [Fix] Fix bogus case/when syntax. Levels 1, 2, and 6 previously defaulted to UNKNOWN (jjowdy)

## 0.14.11 (2024-07-10)
- [Fix] Switch to local release of librdkafka to mitigate its unavailability.

## 0.14.10 (2024-02-08)
- [Fix] Background logger stops working after forking causing memory leaks (mensfeld).

## 0.14.9 (2024-01-29)
- [Fix] Partition cache caches invalid `nil` result for `PARTITIONS_COUNT_TTL`.
- [Enhancement] Report `-1` instead of `nil` in case `partition_count` failure.

## 0.14.8 (2024-01-24)
- [Enhancement] Provide support for Nix OS (alexandriainfantino)
- [Enhancement] Skip intermediate array creation on delivery report callback execution (one per message) (mensfeld)

## 0.14.7 (2023-12-29)
- [Fix] Recognize that Karafka uses a custom partition object (fixed in 2.3.0) and ensure it is recognized.

## 0.14.6 (2023-12-29)
- **[Feature]** Support storing metadata alongside offsets via `rd_kafka_offsets_store` in `#store_offset` (mensfeld)
- [Enhancement] Increase the `#committed` default timeout from 1_200ms to 2000ms. This will compensate for network glitches and remote clusters operations and will align with metadata query timeout.

## 0.14.5 (2023-12-20)
- [Enhancement] Provide `label` producer handler and report reference for improved traceability.

## 0.14.4 (2023-12-19)
- [Enhancement] Add ability to store offsets in a transaction (mensfeld)

## 0.14.3 (2023-12-17)
- [Enhancement] Replace `rd_kafka_offset_store` with `rd_kafka_offsets_store` (mensfeld)
- [Fix] Missing ACL `RD_KAFKA_RESOURCE_BROKER` constant reference (mensfeld)
- [Change] Rename `matching_acl_pattern_type` to `matching_acl_resource_pattern_type` to align the whole API (mensfeld)

## 0.14.2 (2023-12-11)
- [Enhancement] Alias `topic_name` as `topic` in the delivery report (mensfeld)
- [Fix] Fix return type on `#rd_kafka_poll` (mensfeld)
- [Fix] `uint8_t` does not exist on Apple Silicon (mensfeld)

## 0.14.1 (2023-12-02)
- **[Feature]** Add `Admin#metadata` (mensfeld)
- **[Feature]** Add `Admin#create_partitions` (mensfeld)
- **[Feature]** Add `Admin#delete_group` utility (piotaixr)
- **[Feature]** Add Create and Delete ACL Feature To Admin Functions (vgnanasekaran)
- **[Enhancement]** Improve error reporting on `unknown_topic_or_part` and include missing topic (mensfeld)
- **[Enhancement]** Improve error reporting on consumer polling errors (mensfeld)

## 0.14.0 (2023-11-17)
- [Enhancement] Bump librdkafka to 2.3.0
- [Enhancement] Increase the `#lag` and `#query_watermark_offsets` default timeouts from 100ms to 1000ms. This will compensate for network glitches and remote clusters operations.

## 0.13.10 (2024-07-10)
- [Fix] Switch to local release of librdkafka to mitigate its unavailability.

## 0.13.9 (2023-11-07)
- [Enhancement] Expose alternative way of managing consumer events via a separate queue.
- [Enhancement] Allow for setting `statistics_callback` as nil to reset predefined settings configured by a different gem.

## 0.13.8 (2023-10-31)
- [Enhancement] Get consumer position (thijsc & mensfeld)

## 0.13.7 (2023-10-31)
- [Change] Drop support for Ruby 2.6 due to incompatibilities in usage of `ObjectSpace::WeakMap`
- [Fix] Fix dangling Opaque references.

## 0.13.6 (2023-10-17)
- **[Feature]** Support transactions API in the producer
- [Enhancement] Add `raise_response_error` flag to the `Rdkafka::AbstractHandle`.
- [Enhancement] Provide `#purge` to remove any outstanding requests from the producer.
- [Enhancement] Fix `#flush` does not handle the timeouts errors by making it return true if all flushed or false if failed. We do **not** raise an exception here to keep it backwards compatible.

## 0.13.5
- Fix DeliveryReport `create_result#error` being nil despite an error being associated with it

## 0.13.4
- Always call initial poll on librdkafka to make sure oauth bearer cb is handled pre-operations.

## 0.13.3
- Bump librdkafka to 2.2.0

## 0.13.2
- Ensure operations counter decrement is fully thread-safe
- Bump librdkafka to 2.1.1

## 0.13.1
- Add offsets_for_times method on consumer (timflapper)

## 0.13.0 (2023-07-24)
- Support cooperative sticky partition assignment in the rebalance callback (methodmissing)
- Support both string and symbol header keys (ColinDKelley)
- Handle tombstone messages properly (kgalieva)
- Add topic name to delivery report (maeve)
- Allow string partitioner config (mollyegibson)
- Fix documented type for DeliveryReport#error (jimmydo)
- Bump librdkafka to 2.0.2 (lmaia)
- Use finalizers to cleanly exit producer and admin (thijsc)
- Lock access to the native kafka client (thijsc)
- Fix potential race condition in multi-threaded producer (mensfeld)
- Fix leaking FFI resources in specs (mensfeld)
- Improve specs stability (mensfeld)
- Make metadata request timeout configurable (mensfeld)
- call_on_partitions_assigned and call_on_partitions_revoked only get a tpl passed in (thijsc)
- Support `#assignment_lost?` on a consumer to check for involuntary assignment revocation (mensfeld)
- Expose `#name` on the consumer and producer (mensfeld)
- Introduce producer partitions count metadata cache (mensfeld)
- Retry metadta fetches on certain errors with a backoff (mensfeld)
- Do not lock access to underlying native kafka client and rely on Karafka granular locking (mensfeld)

## 0.12.4 (2024-07-10)
- [Fix] Switch to local release of librdkafka to mitigate its unavailability.

## 0.12.3
- Include backtrace in non-raised binded errors.
- Include topic name in the delivery reports

## 0.12.2
- Increase the metadata default timeout from 250ms to 2 seconds. This should allow for working with remote clusters.

## 0.12.1
- Bumps librdkafka to 2.0.2 (lmaia)
- Add support for adding more partitions via Admin API

## 0.12.0 (2022-06-17)
- Bumps librdkafka to 1.9.0
- Fix crash on empty partition key (mensfeld)
- Pass the delivery handle to the callback (gvisokinskas)

## 0.11.0 (2021-11-17)
- Upgrade librdkafka to 1.8.2
- Bump supported minimum Ruby version to 2.6
- Better homebrew path detection

## 0.10.0 (2021-09-07)
- Upgrade librdkafka to 1.5.0
- Add error callback config

## 0.9.0 (2021-06-23)
- Fixes for Ruby 3.0
- Allow any callable object for callbacks (gremerritt)
- Reduce memory allocations in Rdkafka::Producer#produce (jturkel)
- Use queue as log callback to avoid unsafe calls from trap context (breunigs)
- Allow passing in topic configuration on create_topic (dezka)
- Add each_batch method to consumer (mgrosso)

## 0.8.1 (2020-12-07)
- Fix topic_flag behaviour and add tests for Metadata (geoff2k)
- Add topic admin interface (geoff2k)
- Raise an exception if @native_kafka is nil (geoff2k)
- Option to use zstd compression (jasonmartens)

## 0.8.0 (2020-06-02)
- Upgrade librdkafka to 1.4.0
- Integrate librdkafka metadata API and add partition_key (by Adithya-copart)
- Ruby 2.7 compatibility fix (by Geoff Thé)A
- Add error to delivery report (by Alex Stanovsky)
- Don't override CPPFLAGS and LDFLAGS if already set on Mac (by Hiroshi Hatake)
- Allow use of Rake 13.x and up (by Tomasz Pajor)

## 0.7.0 (2019-09-21)
- Bump librdkafka to 1.2.0 (by rob-as)
- Allow customizing the wait time for delivery report availability (by mensfeld)

## 0.6.0 (2019-07-23)
- Bump librdkafka to 1.1.0 (by Chris Gaffney)
- Implement seek (by breunigs)

## 0.5.0 (2019-04-11)
- Bump librdkafka to 1.0.0 (by breunigs)
- Add cluster and member information (by dmexe)
- Support message headers for consumer & producer (by dmexe)
- Add consumer rebalance listener (by dmexe)
- Implement pause/resume partitions (by dmexe)

## 0.4.2 (2019-01-12)
- Delivery callback for producer
- Document list param of commit method
- Use default Homebrew openssl location if present
- Consumer lag handles empty topics
- End iteration in consumer when it is closed
- Add support for storing message offsets
- Add missing runtime dependency to rake

## 0.4.1 (2018-10-19)
- Bump librdkafka to 0.11.6

## 0.4.0 (2018-09-24)
- Improvements in librdkafka archive download
- Add global statistics callback
- Use Time for timestamps, potentially breaking change if you
  rely on the previous behavior where it returns an integer with
  the number of milliseconds.
- Bump librdkafka to 0.11.5
- Implement TopicPartitionList in Ruby so we don't have to keep
  track of native objects.
- Support committing a topic partition list
- Add consumer assignment method

## 0.3.5 (2018-01-17)
- Fix crash when not waiting for delivery handles
- Run specs on Ruby 2.5

## 0.3.4 (2017-12-05)
- Bump librdkafka to 0.11.3

## 0.3.3 (2017-10-27)
- Fix bug that prevent display of `RdkafkaError` message

## 0.3.2 (2017-10-25)
- `add_topic` now supports using a partition count
- Add way to make errors clearer with an extra message
- Show topics in subscribe error message
- Show partition and topic in query watermark offsets error message

## 0.3.1 (2017-10-23)
- Bump librdkafka to 0.11.1
- Officially support ranges in `add_topic` for topic partition list.
- Add consumer lag calculator

## 0.3.0 (2017-10-17)
- Move both add topic methods to one `add_topic` in `TopicPartitionList`
- Add committed offsets to consumer
- Add query watermark offset to consumer

## 0.2.0 (2017-10-13)
- Some refactoring and add inline documentation

## 0.1.x (2017-09-10)
- Initial working version including producing and consuming
