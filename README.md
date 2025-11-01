# Karafka-Rdkafka

[![Build Status](https://github.com/karafka/karafka-rdkafka/actions/workflows/ci_linux_ubuntu_x86_64_gnu.yml/badge.svg)](https://github.com/karafka/karafka-rdkafka/actions/workflows/ci_linux_x86_64_gnu.yml)
[![Gem Version](https://badge.fury.io/rb/karafka-rdkafka.svg)](https://badge.fury.io/rb/karafka-rdkafka)
[![Join the chat at https://slack.karafka.io](https://raw.githubusercontent.com/karafka/misc/master/slack.svg)](https://slack.karafka.io)

> [!NOTE]
> **Karafka-Rdkafka** is a fork of the [rdkafka-ruby](https://github.com/karafka/rdkafka-ruby) gem, specifically maintained for the [Karafka ecosystem](https://karafka.io). This fork exists to provide Karafka-specific patches and optimizations that are not generic enough for inclusion in the upstream rdkafka-ruby library.

---

## Why This Fork Exists

While rdkafka-ruby serves as an excellent general-purpose Kafka client for Ruby, Karafka requires specific optimizations and patches that are:

- **Karafka-specific**: Tailored for Karafka's unique processing patterns and requirements
- **Performance-oriented**: Focused on high-throughput, low-latency scenarios that Karafka specializes in
- **Framework-integrated**: Designed to work seamlessly with Karafka's architecture and features

These modifications are not suitable for the upstream rdkafka-ruby library because they are either too specific to Karafka's use cases or might introduce breaking changes for other users of rdkafka-ruby.

## Maintenance and Synchronization

This fork is actively maintained and kept in sync with the upstream rdkafka-ruby repository as much as possible. We:

- **Regularly merge** upstream changes from rdkafka-ruby
- **Maintain compatibility** with the rdkafka-ruby API wherever possible
- **Apply minimal patches** to avoid diverging significantly from upstream
- **Merge back generic improvements** from karafka-rdkafka to rdkafka-ruby when they benefit the broader community
- **Test thoroughly** to ensure stability within the Karafka ecosystem

## Long-term Plan

Our long-term goal is to work with the rdkafka-ruby maintainers to eventually merge the beneficial changes back upstream. This would allow us to:

- Reduce maintenance overhead
- Benefit the broader Ruby Kafka community
- Simplify the Karafka ecosystem dependencies

However, until such integration is possible, this fork ensures that Karafka users get the best possible performance and reliability.

### If You're Using Standalone rdkafka

You should use the [original rdkafka-ruby gem](https://github.com/karafka/rdkafka-ruby) for general Kafka client needs. This fork is specifically designed for Karafka and may not be suitable for other use cases.

### Reporting Issues

For issues related to this fork, please report them in the [rdkafka-ruby repository](https://github.com/karafka/rdkafka-ruby/issues) rather than here. This helps us:

- Maintain a single place for issue tracking
- Ensure upstream compatibility
- Provide better support for all users

### Contributing

Contributions should generally be made to the upstream [rdkafka-ruby repository](https://github.com/karafka/rdkafka-ruby). Changes to this fork are only made when:

- They are specific to Karafka's requirements
- They cannot be generalized for upstream inclusion
- They are temporary while working on upstream integration

## Versions

| rdkafka-ruby | librdkafka | patches |
|-|-|-|
| 0.23.x (2025-11-01) | 2.12.1 (2025-10-16)  | yes |
| 0.22.x (2025-09-26) | 2.11.1 (2025-08-18)  | yes |
| 0.21.x (2025-08-18) | 2.11.0 (2025-07-03)  | yes |
| 0.20.x (2025-07-17) | 2.8.0  (2025-01-07)  | yes |
| 0.19.x (2025-01-20) | 2.8.0  (2025-01-07)  | yes |
| 0.18.0 (2024-11-26) | 2.6.1  (2024-11-18)  | yes |
| 0.17.4 (2024-09-02) | 2.5.3  (2024-09-02)  | yes |
| 0.17.0 (2024-08-01) | 2.5.0  (2024-07-10)  | yes |
| 0.16.0 (2024-06-13) | 2.4.0  (2024-05-07)  | no  |
| 0.15.0 (2023-12-03) | 2.3.0  (2023-10-25)  | no  |
| 0.14.0 (2023-11-21) | 2.2.0  (2023-07-12)  | no  |
| 0.13.0 (2023-07-24) | 2.0.2  (2023-01-20)  | no  |
| 0.12.0 (2022-06-17) | 1.9.0  (2022-06-16)  | no  |
| 0.11.0 (2021-11-17) | 1.8.2  (2021-10-18)  | no  |
| 0.10.0 (2021-09-07) | 1.5.0  (2020-07-20)  | no  |
