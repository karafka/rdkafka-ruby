# Ext

This gem dependes on the `librdkafka` C library. It is downloaded when
this gem is installed.

To update the `librdkafka` version follow the following steps:

* Download the new version `tar.gz` from  https://github.com/edenhill/librdkafka/
* Generate a `sha256` with (if using MacOS) `shasum -a 256 <file>`
* Change the `sha256` in `lib/rdkafka/version.rb`
* Change the version in `lib/rdkafka/version.rb`

## Disclaimer

Currently the `librdkafka` project does not provide
checksums of releases. The checksum provided here is generated on a best
effort basis. If the CDN would be compromised at the time of download the
checksum could be incorrect.

Do your own verification if you rely on this behaviour.

Once https://github.com/appsignal/rdkafka-ruby/issues/44 is implemented
we will change this process.
