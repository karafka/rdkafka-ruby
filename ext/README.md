# Ext

This gem dependes on the `librdkafka` C library. It is downloaded when
this gem is installed.

To update the `librdkafka` version follow the following steps:

* Go to https://github.com/edenhill/librdkafka/releases to get the new
  version number and asset checksum for `tar.gz`.
* Change the version in `lib/rdkafka/version.rb`
* Change the `sha256` in `lib/rdkafka/version.rb`
* Run `bundle exec rake` in the `ext` directory to download and build
  the new version
* Run `docker-compose pull` in the main gem directory to ensure the docker
  images used by the tests and run `docker-compose up`
* Finally, run `bundle exec rspec` in the main gem directory to execute
  the test suite to detect any regressions that may have been introduced
  by the update
