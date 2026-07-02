# Rdkafka Changelog

## Unreleased
- [Enhancement] Extract the admin background-event result handlers into one class per operation under `lib/rdkafka/callbacks/` (`CreateTopicHandler`, `DescribeConfigsHandler`, etc., all subclasses of `Callbacks::BaseHandler`). `Callbacks::BackgroundEventCallback` is now a thin dispatcher that maps the event type to its handler and destroys the event. Purely internal reorganization (`Rdkafka::Callbacks` is private) with no behavior or API change.
- [Enhancement] Expose `replicas` and `isrs` (in-sync replica broker ids) on each partition in topic metadata (`Metadata#topics` partition hashes). The base struct `#to_h` skips FFI pointer members, so these two arrays were dropped entirely and the partition replica assignment was unavailable to callers (e.g. for planning replication-factor changes). `PartitionMetadata#to_h` now dereferences both pointers into arrays of broker ids.
- [Enhancement] Reuse per-thread scratch pointers in `Consumer::Headers.from_native` instead of allocating them for every consumed message. Previously each message paid one native pointer allocation just to check for headers and three more when headers were present; now the scratch pointers are allocated once per thread/fiber and reused, removing all per-message native scratch allocations from the consumer hot path.
- [Enhancement] Remove the unused `DeliveryHandle` `:topic_name` struct field and the per-message allocation that populated it. The delivery callback copied the topic name into a native `FFI::MemoryPointer` on every delivered message, retained for the lifetime of the handle, yet nothing ever read it: the topic is already available via `DeliveryHandle#topic` (a Ruby attribute set during `produce`) and `DeliveryReport#topic_name`, both of which work exactly as before.
- [Fix] Add the missing `closed_consumer_check` to `Consumer#position`. Every sibling offset method guards against a closed consumer, but `position` did not: with an explicit `list` it raised `ClosedInnerError` instead of `ClosedConsumerError`, and with no `list` it raised `ClosedConsumerError` naming `assignment` rather than `position`. It now raises a consistent `ClosedConsumerError` for `position`.
- [Fix] Stop leaking the native `rd_kafka_topic_conf_t` in `Producer#set_topic_config` when a per-topic config value is rejected. The conf is built with `rd_kafka_topic_conf_new` and only handed to `rd_kafka_topic_new` afterwards; if `rd_kafka_topic_conf_set` returned a non-`:config_ok` result the method raised before `rd_kafka_topic_new` was ever called, so librdkafka never took ownership of the conf and it leaked (repeatable per produce with an invalid `topic_config:`). The conf is now destroyed before raising. The `rd_kafka_topic_new` path is unchanged: librdkafka frees the conf there on both success and failure, so it is never destroyed by us.
- [Fix] Raise instead of silently dropping a rejected `incremental_alter_configs` entry. `rd_kafka_ConfigResource_add_incremental_config` returns an `rd_kafka_error_t` for an invalid op_type, an empty/nil name, or a nil value on a non-delete op; the result was ignored, so the entry was dropped, the alter request still reported success, and the error object leaked. The error is now surfaced as an `RdkafkaError` (raised before the request is sent) and the native error object is freed.
- [Fix] Let `PartitionsCountCache` adopt a lower partition count once the cached entry has expired. The cache prioritizes higher counts (partition counts only grow during normal operation), but it did so unconditionally: after the TTL expired it fetched the true lower count, discarded it, and re-armed the TTL on the stale higher count - permanently. A topic recreated with fewer partitions then made `produce` (with a partition key) fail with `unknown_partition` for the dropped partitions until process restart. A lower value is now adopted on the first refresh after expiry, while still being ignored within the TTL window so a transient or racy lower read cannot clobber a correct higher count.
- [Fix] Make the `Consumer` GC finalizer close the consumer and destroy its consumer queue, not just the native client. The finalizer used the generic `NativeKafka` one, which went straight to `rd_kafka_destroy` and skipped `rd_kafka_consumer_close` and `rd_kafka_queue_destroy`. A consumer that used `poll_batch` (which takes a consumer-queue reference) and was then garbage-collected without an explicit `close` left that reference dangling, which could make `rd_kafka_destroy` block inside the finalizer (process hang at GC/shutdown) or leak the handle. The consumer now installs its own finalizer that mirrors `#close` (the queue pointer is shared with the finalizer via a holder so it never captures the consumer and prevents collection).
- [Fix] Stop admin operations from leaking when their arguments are rejected. `describe_configs` and `incremental_alter_configs` allocated the background queue and AdminOptions and registered their handle before parsing input, so a missing key raised a `KeyError` out of the building loop that orphaned the handle in the process-global registry forever (and leaked the queue, AdminOptions and any `ConfigResource`s already built); `list_offsets` likewise allocated its native topic-partition list before validating the offset specs. All three now parse and validate input up front, so a bad argument raises with nothing allocated.
- [Fix] Destroy the native topic-partition list in `TopicPartitionList#to_native_tpl` when population fails partway. The list is allocated with `rd_kafka_topic_partition_list_new` and only handed back (for the caller to destroy) on success; if building it raised - e.g. a non-string metadata value or an offset FFI cannot coerce to int64 - the half-built list leaked, since destruction is fully manual (the previous doc comment claiming GC handled it was wrong). It is now destroyed before the error propagates.
- [Fix] Allocate the admin result-count out-parameter as `:size_t` instead of `:int32`. Every librdkafka `rd_kafka_*_result_*(result, size_t *cntp)` accessor writes a full native `size_t` (8 bytes on 64-bit), but the count pointer was a 4-byte `FFI::MemoryPointer.new(:int32)` across the create/delete topic, create partitions, create/delete ACL, describe/incremental-alter configs and config-synonyms paths - a 4-byte heap overflow on every admin result parse (benign on little-endian, where the low word still reads the correct count, but undefined behavior). Now uses `:size_t`, matching the already-correct list-offsets and `get_err_descs` paths.
- [Fix] Destroy admin API background events after processing. librdkafka requires the application to destroy each background event, but `rd_kafka_event_destroy` was never called (nor even bound), so every admin operation leaked its entire result event with all result arrays and strings. Reports are now built inside the callback (copying event-owned memory into Ruby objects) before the event is destroyed, and `DescribeConfigsReport`/`IncrementalAlterConfigsReport` no longer destroy the event-owned ConfigResource array, which also fixes a double free on repeated `wait` calls on the same handle. As part of this, the internal FFI struct fields on admin operation handles (e.g. `handle[:error_string]`, `handle[:result_name]`, `handle[:config_entries]`, `handle[:response_string]`, `handle[:matching_acls]`) were removed; they were never part of the public API (use `handle.wait` and the returned report objects, whose interfaces are unchanged).
- [Fix] Resolve admin operation handles from the event error when an admin operation fails at the operation level (e.g. brokers unreachable, or the client closed with the request in flight). librdkafka delivers such failures as a result event with the error set and an empty results array, but the create topic, delete topic, create partitions, delete groups, create ACL and delete ACL handlers indexed `results[0]` unconditionally. That raised inside the background event callback, so the handle was never unlocked and `wait` blocked until its own timeout and raised `WaitTimeoutError`, discarding the real error. These handlers now check the event error first and resolve the handle with the actual error code (the describe configs, incremental alter configs, describe ACL and list offsets handlers already did).
- [Fix] Stop leaking the native `rd_kafka_conf_t` when client creation fails. `native_config` raised `ConfigError` mid-build (e.g. on an invalid option) without destroying the conf, and `native_kafka` raised `ClientCreationError` on a null `rd_kafka_new` without destroying it either. Both paths now call `rd_kafka_conf_destroy` before re-raising (librdkafka keeps app ownership of the conf on `rd_kafka_new` failure, so this is safe). Multi-KB leak per failed creation, relevant for supervisors retrying client creation on transient SASL/SSL misconfig.
- [Fix] Stop `Metadata` from leaking the native metadata struct (and a topic reference) on every retried fetch. `retry` restarts the `begin` block without running its `ensure`, so each retried attempt reassigned the pointers and only the last attempt's `rd_kafka_metadata` struct was ever destroyed; up to `METADATA_MAX_RETRIES` whole-cluster structs leaked per call (the `leader_not_available` case is routine during topic creation/leader election). Each attempt now frees its own native resources, and a failed fetch that never allocated a struct no longer calls `rd_kafka_metadata_destroy` on a NULL pointer.
- [Fix] Free the librdkafka-allocated string in `Consumer#cluster_id` and `Consumer#member_id` (previously copied via a `:string` binding but never freed) and fix the `rd_kafka_clusterid` arity to pass `timeout_ms`. `Consumer#cluster_id` now accepts a `timeout_ms` (default `Defaults::CONSUMER_CLUSTER_ID_TIMEOUT_MS`).
- [Fix] Guard the message delivery callback so a raising user `delivery_callback` can no longer skip the handle unlock or crash the producer. `DeliveryCallback` invoked the user callback and only then unlocked the handle, with no rescue; if the callback raised, the handle stayed pending (so `wait` blocked until its timeout and raised `WaitTimeoutError` for a message that was actually delivered) and the exception unwound out of the FFI callback on librdkafka's polling thread (`abort_on_exception = true`), taking down the whole process. The user callback is now wrapped so exceptions are logged and swallowed (matching the rebalance callback) and the handle is always unlocked in an `ensure`.
- [Fix] Stop `Producer#produce` from orphaning the delivery handle in the process-global registry when it fails after registering it. The handle was only removed on a non-zero `rd_kafka_producev` return, so any exception between registration and that check (a concurrent `close` making `with_inner` raise `ClosedInnerError`, or a header value whose `#to_s` raises) leaked the handle forever - it survives producer close and accumulates in apps that recreate/close producers. `produce` now removes the handle on any such failure before re-raising.
- [Fix] Attach `rd_kafka_query_watermark_offsets` with `blocking: true` so it releases the GVL during its broker round-trip. It was the only synchronous network call bound without the flag, so `Consumer#query_watermark_offsets` (and `Consumer#lag`, which calls it once per partition) froze every other Ruby thread in the process - including producer polling threads - for up to `timeout_ms`. Matches the neighboring `rd_kafka_offsets_for_times` binding.
- [Fix] Raise instead of silently dropping a rejected `incremental_alter_configs` entry. `rd_kafka_ConfigResource_add_incremental_config` returns an `rd_kafka_error_t` for an invalid op_type, an empty/nil name, or a nil value on a non-delete op; the result was ignored, so the entry was dropped, the alter request still reported success, and the error object leaked. The error is now surfaced as an `RdkafkaError` (raised before the request is sent) and the native error object is freed.
- [Fix] Stabilize the flaky `Consumer#lag` "calculates the consumer lag" spec on overloaded CI. The manual `consumer.commit` could raise `no_offset` when the default 5s background auto-commit had already committed the stored offsets, or when the auto offset store had not yet caught up with `poll`. The spec now raises `auto.commit.interval.ms` to 60s (matching the existing `#seek`/pause specs) and lets the offset store settle before committing. Backported from rdkafka-ruby (#912).
- [Fix] Fix the NULL background-queue cleanup branches in `Admin#delete_group`, `Admin#delete_acl` and `Admin#describe_acl`, which referenced undefined local variables (`delete_topic_ptr`/`new_acl_ptr`). When `rd_kafka_queue_get_background` returned NULL, those branches raised a `NameError` instead of the intended `ConfigError` and leaked the already-allocated native request object (the `rd_kafka_DeleteGroup_t` / ACL binding filter), and `delete_group` additionally called the wrong destructor. They now destroy the correct object and raise `ConfigError`.
- [Fix] Forward `broker_message` and `instance_name` through `RdkafkaError.build`. On the `rd_kafka_error_t` pointer path `build` called `build_from_c` without passing either, so a caller-supplied broker message was discarded (falling back to `rd_kafka_err2str`) and the instance name was lost; the `Bindings::Message` path also dropped `instance_name`. Both are now forwarded, and `build_from_c` accepts `instance_name`.
- [Fix] Synchronize `AbstractHandle::REGISTRY` mutations. The handle registry is a plain Hash mutated from producing/consuming threads and the background polling thread (which removes handles from FFI callbacks). That is effectively safe on MRI under the GVL but not on JRuby, where a lost write could leave a handle unregistered (never unlocked, so `wait` times out for a delivered message) or never removed (permanent leak). `register`/`remove` now guard the Hash with a mutex.
- [Fix] Stop the `Metadata` retry loop from clobbering the request timeout and blocking for minutes. Each retry overwrote the per-request timeout with the exponential backoff value, so the first retries ran with a far-too-short request timeout (e.g. 200ms vs the 2,000ms default, near-guaranteeing another timeout) while later ones inflated it to ~100s; the cumulative sleeps alone reached ~204s. The request timeout is now left unchanged across retries, the per-attempt backoff is capped at `Defaults::METADATA_RETRY_BACKOFF_MAX_MS` (1,000ms), and the loop is bounded by a wall-clock budget (`Defaults::METADATA_RETRY_BUDGET_MS`, 5,000ms) after a floor of `Defaults::METADATA_MIN_ATTEMPTS` (3) tries. This keeps a synchronous metadata fetch from blocking for minutes while still giving a slow broker a few attempts: fast-recovering clusters retry quickly within the budget, an unresponsive one fails in ~5s (a bit over if its requests each consume the full timeout), and at least 3 attempts always happen.

## 0.27.2 (2026-05-21)
- [Enhancement] `poll_batch` and `poll_batch_nb` now return error events inline as `RdkafkaError` objects rather than raising on the first error. The return type is `Array<Message, RdkafkaError>` and callers are responsible for handling errors in the result.

## 0.27.1 (2026-05-14)
- [Fix] `poll_nb`, `poll_nb_each`, `poll_batch`, and `poll_batch_nb` now raise `RdkafkaError` with `details` populated (`{topic:, partition:, offset:}`) when a message contains an error (e.g. `:partition_eof`). Previously these methods raised via `RdkafkaError.new(code)`, discarding the native message struct context. They now use `RdkafkaError.validate!(native_message, client_ptr: inner)`, consistent with `poll`.

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
