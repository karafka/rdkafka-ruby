# Ext

This gem dependes on the `librdkafka` C library. It is downloaded when
this gem is installed.

To update the `librdkafka` version follow the following steps:

* Go to https://github.com/edenhill/librdkafka/releases to get the new
  version number and asset checksum for `tar.gz`.
* Change the version in `lib/rdkafka/version.rb`
* Change the `sha256` in `lib/rdkafka/version.rb`
