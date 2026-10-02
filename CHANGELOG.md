# Rdkafka Changelog

## 0.30.3 (Unreleased)
- [Enhancement] Bump the bundled OpenSSL to `3.5.9` (LTS) for its security fixes. Ported from rdkafka-ruby (#1007).

## 0.30.2 (2026-10-01)
- [Feature] Add `ShareConsumer#events_poll` (and `#events_poll_nb`) to service the statistics, error, log and OAuthBearer callbacks without acquiring records.

## 0.30.1 (2026-09-30)
- [Fix] Derive `ShareConsumer#name` from the native handle at creation (mirroring `Consumer#name`) instead of leaving it `nil`, so downstreams that route the global statistics and error callbacks by client name (e.g. Karafka) no longer drop share-consumer statistics and background errors.

## 0.30.0 (2026-09-25)
- [Fix] Register share consumers in `Rdkafka::Clients` and destroy the native handle when `Config#share_consumer` fails part-way, so a share consumer is no longer missed by the `at_exit` shutdown hook.
- [Feature] Add preview support for KIP-932 share groups ("Queues for Kafka") via `Rdkafka::ShareConsumer`, created with `Config#share_consumer`. Members consume partitions cooperatively with per-record acknowledgements (`:accept`, `:release`, `:reject`) instead of committed offsets. Like `Consumer`, it is a thin binding over the librdkafka share primitives and drives no poll loop or acknowledgement strategy itself, leaving that to a higher layer such as Karafka. Requires a broker with share groups enabled (Apache Kafka 4.2.0+); librdkafka marks the feature as preview and not production-ready.
- [Enhancement] `RdkafkaError.build_from_c` now uses the human-readable string librdkafka attaches to the `rd_kafka_error_t` instead of `rd_kafka_err2str`, improving diagnostics for every error-pointer path.
- [Enhancement] Refactor the `Helpers::OAuth` token plumbing around two private hooks so the share consumer overrides only those instead of duplicating both public methods.
- [Fix] Make `ShareConsumer#close` thread-safe: it now waits for in-flight operations (mirroring `NativeKafka`), preventing a double destroy when two threads race `close`.
- [Fix] Keep the `ShareConsumer` native handle and its GC finalizer when `close`/`destroy` report an error, so the client is no longer leaked and `close` stays retriable.
- [Fix] Pin the acknowledgement commit callback `FFI::Function` for the lifetime of the `ShareConsumer`, preventing a use-after-free when an unclosed consumer is garbage collected.
- [Fix] Make `ShareConsumer` fork-aware (mirroring `NativeKafka`): it records the creating pid and reports `closed?` in any other process, so a child that inherited the handle no longer segfaults when its finalizer or `close` runs the native teardown.
- [Fix] Scope `ShareConsumer#each`'s `ClosedConsumerError` rescue to the `poll` call, so an error raised inside the caller's block propagates instead of silently ending iteration. Like `#poll`, `#each` can also yield an `RdkafkaError` for a record that fails to build.
- [Fix] Raise `ArgumentError` for `ShareConsumer#subscribe` without topics, which librdkafka otherwise treats as an unsubscribe without error, silently dropping the subscription.
- [Fix] Capture the librdkafka client name onto `ShareConsumer#name` from the OAuthBearer token refresh callback, since the share handle has no native name accessor for the documented name-based oauth callback routing.
- [Fix] Update `ext/build_common.sh` (precompiled builds) to librdkafka `2.15.0` and its tarball checksum; it still pinned `2.14.1`, whose tarball is no longer vendored.
- [Fix] Stop the `share_consumer_multi_member` integration spec from failing on an at-least-once redelivery; it now tracks distinct payloads and fails only when a record is never delivered.
- [Fix] Apply the same at-least-once tolerance to the `share_consumer_explicit_ack` integration spec: require only the released record to be redelivered, forbid the rejected one, and tolerate an accepted record reappearing.
- [Fix] Add the `nofile` ulimit to `docker-compose-ssl.yml` (mirroring `docker-compose.yml`) so the SSL CI broker does not exhaust file descriptors under the topic-heavy share-consumer suite.
- [Fix] Enable KIP-932 share groups on the macOS CI broker, whose KRaft config never opted into the share rebalance protocol, so its share-consumer spec no longer receives zero records. `CONFLUENT_VERSION` is bumped to `8.3.0`.
- [Enhancement] Add `share_consumer_implicit_ack` and `share_consumer_multi_topic` integration specs.
- [Fix] Stop the `statistics_unassigned_producer` integration spec from flaking on a metadata-propagation race by retrying the produce on `unknown_topic_or_part` and `leader_not_available`.
- [Fix] Also stabilize the `statistics_unassigned_producer` integration spec against a delivery wait that outlives its budget: it now uses the default 60s handle wait and retries on `WaitTimeoutError`.
- [Fix] Stabilize the `consumer_memberid_clusterid_leak` integration spec against RSS measurement noise on Ruby 4.0 by compacting the heap before sampling and raising the ceiling to 3 MB.
- [Note] Share consumers emit the regular consumer statistics JSON (including `cgrp`) through the usual `statistics_callback`; librdkafka 2.15.0 exposes no share-specific section or per-partition share metrics, so the `topics` section carries no partition entries and `statistics.unassigned.include` needs no share-consumer special-casing.
- [Feature] Add `Admin#alter_consumer_group_offsets` and `Admin#delete_consumer_group_offsets` to set or clear a consumer group's committed offsets from the admin client. Ported from rdkafka-ruby (#983).
- [Enhancement] Bump librdkafka to `2.15.1` for OpenSSL and libcurl security fixes. It changes how IPv6 addresses are formatted and validated against broker certificates.
- [Enhancement] Bump the bundled zlib to `1.3.2`.
- [Enhancement] Bump the bundled MIT Kerberos (krb5) to `1.22.2`. Ported from rdkafka-ruby (#979).
- [Enhancement] Add `Rdkafka::Config.partitioner_key_uses_bytesize` to hash partition keys by byte length, matching other Kafka clients for multibyte keys. Defaults to `false`. Ported from rdkafka-ruby (#985).
- [Maintenance] Derive the zlib `CHECKSUMS` entry from `ZLIB_VERSION`.
- [Maintenance] Drop the unused `dist/openssl-3.0.16.tar.gz` build cache.
- [Fix] Honor the `isolation_level:` argument to `Admin#list_offsets`, which was silently ignored. Ported from rdkafka-ruby (#983).
- [Fix] Return correct results for every item of a multi-item topic, partition, group or ACL admin request, not just the first. Ported from rdkafka-ruby (#987).
- [Maintenance] Fix a use-after-free in the multi-item admin integration test that read result-name pointers after librdkafka had destroyed the background event, which made it flaky on newer glibc (e.g. Debian trixie). Ported from rdkafka-ruby (#991).
- [Maintenance] Stabilize consumer specs on slow CI runners: wait longer for a partition assignment, raise the `TestTopics.create` admin timeout (a timeout there also tripped the leaked-handle guard), give the non-blocking `poll_nb` message check a generous retry budget instead of a fixed ~2s, and let the long-running consumption spec drain its backlog instead of stopping at a fixed 60s. The assignment wait is tunable via `RDKAFKA_TEST_ASSIGNMENT_TIMEOUT`. Ported from rdkafka-ruby (#990).

## 0.29.0 (2026-09-14)
- [Enhancement] Bump librdkafka to `2.15.0` (staying on `2.15.0` rather than `2.15.1` so users hitting a regression in `2.15.1` have a stable fallback).

## 0.28.2 (2026-09-11)
- [Fix] Close live clients before Ruby shutdown finalization, preventing a possible segfault on exit. Ported from rdkafka-ruby (#964, Alex Selesse).
- [Fix] Destroy the native handle when client construction fails partway, so it is no longer orphaned. Ported from rdkafka-ruby (#964, Alex Selesse).
- [Fix] Fix a double-free in `Consumer#poll_batch`/`#poll_batch_nb` that could abort the process when building a message raised a non-`RdkafkaError`. Ported from rdkafka-ruby (#973).
- [Fix] Free native resources when `Admin#describe_configs`/`#incremental_alter_configs` raise while building their request (e.g. a non-String resource name). Ported from rdkafka-ruby (#973).
- [Fix] Raise `ConfigError` instead of segfaulting when `Admin#describe_configs`/`#incremental_alter_configs` get an empty resource name or a negative resource type. Ported from rdkafka-ruby (#973).
- [Fix] Stop the topic-partition metadata string from being freed while librdkafka still uses it. Ported from rdkafka-ruby (#969, Randy Stauner).
- [Maintenance] Bump the bundled OpenSSL used by the precompiled builds to `3.5.8` (LTS). Ported from rdkafka-ruby (#972, Scott Francis).

## v0.28.1 (2026-09-04)
- [Enhancement] Add `Admin#delete_records` to delete messages in a partition up to a given offset, or all current data with `:end`. Ported from rdkafka-ruby (#956).
- [Enhancement] Add `Admin#list_consumer_groups` for a cluster-wide listing of consumer groups. Ported from rdkafka-ruby (#955).
- [Fix] Make `NativeKafka#close` fork-aware so a forked child no longer segfaults on exit. Handles record their creator pid and skip the native teardown in any other process.
- [Fix] Stabilize the flaky partitions count cache statistics spec.
- [Fix] Stabilize the `to_native_tpl` leak integration spec against RSS measurement noise.

## v0.28.0 (2026-07-12)
- [Enhancement] Bump librdkafka to `2.14.2` (maintenance release with bundled dependency CVE fixes and a fix for duplicate groups in `ListConsumerGroups`).
- [Enhancement] Add `Consumer#metadata` and `Producer#metadata`, mirroring `Admin#metadata`, to fetch cluster metadata without a separate admin connection.
- [Enhancement] Name the failing topic and partition in `RdkafkaError`s raised for per-partition `list_offsets` errors.
- [Enhancement] Add `Consumer#list_offsets`, mirroring `Admin#list_offsets`, and compute `Consumer#lag` with one batched query instead of a roundtrip per partition.
- [Enhancement] Extract the admin background-event result handlers into one class per operation under `lib/rdkafka/callbacks/`. Internal reorganization with no API or behavior change.
- [Enhancement] Expose `replicas` and `isrs` (in-sync replica broker ids) on each partition in topic metadata; both were previously dropped from the `Metadata#topics` partition hashes.
- [Enhancement] Reuse per-thread scratch pointers in `Consumer::Headers.from_native`, removing the per-message native allocations from the consumer hot path.
- [Enhancement] Remove the unused `DeliveryHandle` `:topic_name` struct field and its per-message allocation. Use `DeliveryHandle#topic` or `DeliveryReport#topic_name`, both unchanged.
- [Fix] Stop `poll_batch`/`poll_batch_nb` from discarding a whole batch when one message fails to build. The failure is returned inline as an `RdkafkaError`.
- [Fix] Add the missing `closed_consumer_check` to `Consumer#position`, so it raises a consistent `ClosedConsumerError` like every sibling offset method.
- [Fix] Stop leaking the native `rd_kafka_topic_conf_t` in `Producer#set_topic_config` when a per-topic config value is rejected.
- [Fix] Raise instead of silently dropping a rejected `incremental_alter_configs` entry, which previously left the alter request reporting success.
- [Fix] Let `PartitionsCountCache` adopt a lower partition count once the cached entry has expired, so a topic recreated with fewer partitions no longer fails `produce` until process restart.
- [Fix] Fully close a consumer collected without an explicit `close`, preventing a hang or leak.
- [Fix] Stop `describe_configs`, `incremental_alter_configs` and `list_offsets` from leaking native resources when their arguments are rejected.
- [Fix] Destroy the native topic-partition list in `TopicPartitionList#to_native_tpl` when population fails partway, which previously leaked the half-built list.
- [Fix] Allocate the admin result-count out-parameter as `:size_t` instead of `:int32`, fixing a 4-byte overflow on every admin result parse.
- [Fix] Stop every admin operation from leaking its result event. The internal FFI struct fields on admin handles are removed; use `handle.wait` and the returned report objects, which are unchanged.
- [Fix] Return the real error from an admin operation that fails at the operation level (e.g. brokers unreachable) instead of blocking until `wait` times out.
- [Fix] Stop leaking the native `rd_kafka_conf_t` when client creation fails, a multi-KB leak per failed attempt for supervisors retrying on transient SASL/SSL misconfiguration.
- [Fix] Stop `Metadata` from leaking the native metadata struct on every retried fetch; each attempt now frees its own native resources.
- [Fix] Free the librdkafka-allocated string in `Consumer#cluster_id` and `Consumer#member_id`, and fix the `rd_kafka_clusterid` arity. `Consumer#cluster_id` now accepts a `timeout_ms`.
- [Fix] Guard the message delivery callback so a raising user `delivery_callback` can no longer leave the handle locked or crash the producer; exceptions are now logged and swallowed.
- [Fix] Stop `Producer#produce` from orphaning the delivery handle in the process-global registry when it fails after registering it.
- [Fix] Attach `rd_kafka_query_watermark_offsets` with `blocking: true` so it releases the GVL; it previously froze every other Ruby thread for up to `timeout_ms`.
- [Fix] Stabilize the flaky `Consumer#lag` spec on overloaded CI. Backported from rdkafka-ruby (#912).
- [Fix] Raise `ConfigError` instead of `NameError` in `Admin#delete_group`, `#delete_acl` and `#describe_acl` when the background queue is unavailable.
- [Fix] Forward `broker_message` and `instance_name` through `RdkafkaError.build`; both were previously discarded on the `rd_kafka_error_t` pointer path.
- [Fix] Synchronize `AbstractHandle::REGISTRY` mutations with a mutex, which could otherwise lose a write on JRuby and leave a handle unregistered or leaked.
- [Fix] Bound the `Metadata` retry loop to about 5 seconds, so a synchronous metadata fetch can no longer block for minutes.
- [Fix] Cache the partition count for a missing topic, so `produce` with a `partition_key` to a not-yet-created topic no longer runs a blocking metadata query on every message.

## 0.27.2 (2026-05-21)
- [Enhancement] `poll_batch` and `poll_batch_nb` now return error events inline as `RdkafkaError` objects rather than raising on the first error. The return type is `Array<Message, RdkafkaError>` and callers are responsible for handling errors in the result.

## 0.27.1 (2026-05-14)
- [Fix] `poll_nb`, `poll_nb_each`, `poll_batch` and `poll_batch_nb` now raise `RdkafkaError` with `details` populated (`{topic:, partition:, offset:}`) when a message contains an error, consistent with `poll`.

## 0.27.0 (2026-05-08)
- [Feature] Add `Consumer#poll_batch(timeout_ms, max_items:)` and `Consumer#poll_batch_nb(timeout_ms, max_items:)` for batch message polling via `rd_kafka_consume_batch_queue` (from upstream).
- [Enhancement] Bump librdkafka to `2.14.1`.
- [Fix] Fix resource leak in `Admin#describe_configs` and `Admin#incremental_alter_configs` where `admin_options_ptr` and `queue_ptr` were not destroyed in the ensure block (from upstream).
- [Fix] Fix leaked queue reference in `Config#native_kafka` where `rd_kafka_queue_get_main` return value was not destroyed after passing to `rd_kafka_set_log_queue` (from upstream).
- [Fix] Fix native topic partition list leak in `Consumer#position` where `tpl` was never destroyed (from upstream).

## 0.26.1 (2026-04-13)
- [Feature] Add `Config#describe_properties` to dump all librdkafka configuration properties (including defaults and hidden properties) as a Hash via `rd_kafka_conf_dump` (from upstream).

## 0.26.0 (2026-04-11)
- [Enhancement] Bump librdkafka to `2.14.0`.
- [Enhancement] Add `advertised.listeners` to macOS ARM64 CI KRaft broker config to fix flaky tests (from upstream).

## 0.25.0 (2026-04-02)
- **[Feature]** Support `rd_kafka_ListOffsets` admin API for querying partition offsets by specification (earliest, latest, max_timestamp, or by timestamp) without requiring a consumer group (from upstream).
- **[Feature]** Extend `Rdkafka::RdkafkaError` with `instance_name` attribute containing the `rd_kafka_name` for tying errors back to specific native Kafka instances (from upstream).
- [Enhancement] Bump librdkafka to `2.13.2`
- [Enhancement] Update `confluentinc/cp-kafka` Docker image to `8.2.0` (from upstream).
- [Enhancement] Disable broker-side auto topic creation to prevent race condition warnings with Kafka 8.2.0 (from upstream).
- [Enhancement] Embed a per-file SPEC_HASH in test topic and consumer group names for tracing Kafka warnings back to specific spec files (from upstream).
- [Fix] Fix test topic auto-creation race conditions causing `TOPIC_ALREADY_EXISTS` warnings (from upstream).
- [Fix] Fix `describe_configs` specs to not depend on config ordering (from upstream).
- [Fix] Register `ObjectSpace.define_finalizer` in `Rdkafka::Consumer` to prevent segfaults when a consumer is GC'd without being explicitly closed (from upstream).
- [Fix] Remove dead `#finalizer` instance methods from `Consumer` and `Admin` that could never work as GC finalizers (from upstream).
- [Fix] Prevent cascading test failures in admin specs when a single handle leaks into the registry (from upstream).

## 0.24.0 (2026-02-25)
- **[Feature]** Add `Producer#queue_size` (and `#queue_length` alias) to report the number of messages waiting in the librdkafka output queue. Useful for monitoring producer backpressure, implementing custom flow control, debugging message delivery issues, and graceful shutdown logic.
- **[Feature]** Add fiber scheduler API for integration with Ruby fiber schedulers (Falcon, Async) and custom event loops (from upstream). Expose `enable_queue_io_events` and `enable_background_queue_io_events` methods on `Consumer`, `Producer`, and `Admin`.
- **[Deprecation]** `AbstractHandle#wait` parameter `max_wait_timeout` (seconds) is deprecated in favor of `max_wait_timeout_ms` (milliseconds). The old parameter still works with backwards compatibility but will be removed in v1.0.0.
- **[Deprecation]** `PartitionsCountCache` constructor parameter `ttl` (seconds) is deprecated in favor of `ttl_ms` (milliseconds). The old parameter still works with backwards compatibility but will be removed in v1.0.0.
- [Enhancement] Add Ruby 4.0 support.
- [Enhancement] Add `Rdkafka::Defaults` module with centralized timeout constants (aligning with upstream refactor).
- [Enhancement] Add `run_polling_thread` parameter to `Config#producer` and `Config#admin` for fiber scheduler integration (from upstream).
- [Enhancement] Extract all hardcoded timeout values to named constants for better maintainability and discoverability.
- [Enhancement] Add `timeout_ms` parameter to `Consumer#each` for configurable poll timeout (from upstream).
- [Enhancement] Extract non-time configuration values (`METADATA_MAX_RETRIES`, `PARTITIONS_COUNT_CACHE_TTL_MS`) to `Rdkafka::Defaults` module (from upstream).
- [Enhancement] Add descriptive error messages for glibc compatibility issues with instructions for resolution (from upstream).
- [Enhancement] Use native ARM64 runners instead of QEMU emulation for Alpine musl aarch64 builds, improving build performance and reliability (from upstream).
- [Enhancement] Enable parallel compilation (`make -j$(nproc)`) for ARM64 Alpine musl builds (from upstream).
- [Enhancement] Bump librdkafka to 2.13.0.
- [Enhancement] Add non-blocking poll methods (`poll_nb`, `events_poll_nb`) that skip GVL release for efficient fiber scheduler integration when using `poll(0)` (from upstream).
- [Enhancement] Add `events_poll_nb_each` method on `Producer`, `Consumer`, and `Admin` for polling events in a single GVL/mutex session. Yields count after each iteration, caller returns `:stop` to break (from upstream).
- [Enhancement] Add `poll_nb_each` method on `Consumer` for non-blocking message polling with proper resource cleanup, yielding each message and supporting early termination via `:stop` return value (from upstream).
- [Fix] Fix Kerberos build on Alpine 3.23+ (GCC 15/C23) by forcing C17 semantics to maintain compatibility with old-style K&R declarations in MIT Kerberos and Cyrus SASL dependencies.

## 0.23.1 (2025-11-14)
- **[Feature]** Add integrated fatal error handling in `RdkafkaError.validate!` - automatically detects and handles fatal errors (-150) with single entrypoint API.
- [Enhancement] Add optional `client_ptr` parameter to `validate!` for automatic fatal error remapping to actual underlying error codes.
- [Enhancement] Update all Producer and Consumer `validate!` calls to provide `client_ptr` for comprehensive fatal error handling.
- [Enhancement] Add `rd_kafka_fatal_error()` FFI binding to retrieve actual fatal error details.
- [Enhancement] Add `rd_kafka_test_fatal_error()` FFI binding for testing fatal error scenarios.
- [Enhancement] Add `RdkafkaError.build_fatal` class method for centralized fatal error construction.
- [Enhancement] Add comprehensive tests for fatal error handling including unit tests and integration tests.
- [Enhancement] Add `RD_KAFKA_PARTITION_UA` constant for unassigned partition (-1).
- [Enhancement] Replace magic numbers with named constants: use `RD_KAFKA_RESP_ERR_NO_ERROR` instead of `0` for error code checks (18 instances) and `RD_KAFKA_PARTITION_UA` instead of `-1` for partition values (9 instances) across the codebase for better code clarity and maintainability.
- [Enhancement] Add `Rdkafka::Testing` module for testing fatal error scenarios on both producers and consumers.
- [Deprecated] `RdkafkaError.validate_fatal!` - use `validate!` with `client_ptr` parameter instead.

## 0.23.0 (2025-11-01)
- [Enhancement] Bump librdkafka to 2.12.1.
- [Enhancement] Force lock FFI to 1.17.1 or higher to include critical bug fixes around GCC, write barriers, and thread restarts for forks.
- [Fix] Fix for Core dump when providing extensions to oauthbearer_set_token (dssjoblom)

## 0.22.2 (2025-10-09)
- [Fix] Fix Github Action Ruby reference preventing non-compiled releases.

## 0.22.1 (2025-10-09)
- [Enhancement] Optimize header processing to eliminate double hash lookups and method checking overhead.
- [Enhancement] Optimize producer header processing with early returns and efficient array operations (69% faster for nil headers, 41% faster for empty headers, 12-32% faster when headers are present, with larger improvements for complex header scenarios).

## 0.22.0 (2025-09-26)
- **[EOL]** Drop support for Ruby 3.1 to move forward with the fiber scheduler work.
- [Enhancement] Bump librdkafka to 2.11.1.
- [Enhancement] Improve sigstore attestation for precompiled releases.
- [Fix] Fix incorrectly set default SSL certs dir.
- [Fix] Disable OpenSSL Heartbeats during compilation.

## 0.21.0 (2025-08-18)
- [Enhancement] Support explicit Debian testing due to lib issues.
- [Enhancement] Support ARM64 Gnu precompilation.
- [Enhancement] Bump librdkafka to 2.11.0.
- [Enhancement] Improve what symbols are exposed outside of the precompiled extensions.
- [Enhancement] Introduce an integration suite layer for non RSpec specs execution.
- [Fix] Add `json` gem as a dependency (was missing but used).

## 0.20.1 (2025-07-17)
- [Enhancement] Drastically increase number of platforms in the integration suite
- [Fix] Support Ubuntu `22.04` and older Alpine precompiled versions
- [Fix] FFI::DynamicLibrary.load_library': Could not open library
- [Change] Add new CI action to trigger auto-doc refresh.

## 0.20.0 (2025-07-17)
- **[Feature]** Add precompiled `x86_64-linux-gnu` setup.
- **[Feature]** Add precompiled `x86_64-linux-musl` setup.
- **[Feature]** Add precompiled `macos_arm64` setup.
- [Enhancement] Run all specs on each of the platforms with and without precompilation.
- [Enhancement] Support transactional id in the ACL API.
- [Fix] Fix a case where using empty key on the `musl` architecture would cause a segfault.
- [Fix] Fix for null pointer reference bypass on empty string being too wide causing segfault.

**Note**: Precompiled extensions are a new feature in this release. While they significantly improve installation speed and reduce build dependencies, they should be thoroughly tested in your staging environment before deploying to production. If you encounter any issues with precompiled extensions, you can fall back to building from sources. For more information, see the [Native Extensions documentation](https://karafka.io/docs/Development-Native-Extensions/).

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
- **[EOL]** Drop Ruby 3.0 support
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
- **[EOL]** Remove support for Ruby 2.7. Supporting it was a bug since rest of the karafka ecosystem no longer supports it.

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
- **[EOL]** Drop support for Ruby 2.6 due to incompatibilities in usage of `ObjectSpace::WeakMap`
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
- **[EOL]** Bump supported minimum Ruby version to 2.6
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
