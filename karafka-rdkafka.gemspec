# frozen_string_literal: true

require File.expand_path('lib/rdkafka/version', __dir__)

Gem::Specification.new do |gem|
  gem.authors = ['Thijs Cadier', 'Maciej Mensfeld']
  gem.email = ["contact@karafka.io"]
  gem.description = "Modern Kafka client library for Ruby based on librdkafka"
  gem.summary = "The rdkafka gem is a modern Kafka client library for Ruby based on librdkafka. It wraps the production-ready C client using the ffi gem and targets Kafka 1.0+ and Ruby 2.7+."
  gem.license = 'MIT'

  gem.test_files = gem.files.grep(%r{^(test|spec|features)/})
  gem.name = 'karafka-rdkafka'
  gem.require_paths = ['lib']
  gem.version = Rdkafka::VERSION
  gem.required_ruby_version = '>= 3.1'

  if ENV['RUBY_PLATFORM']
    gem.platform = ENV['RUBY_PLATFORM']
    gem.files = `git ls-files`.split($\)

    # Do not include the source code for librdkafka as it should be precompiled already per
    # platform. Same applies to any possible patches.
    gem.files = gem.files.reject do |file|
      file.match?(%r{^dist/librdkafka-.*\.tar\.gz$}) ||
      file.match?(%r{^dist/patches/})
    end

    # Add the compiled extensions that exist (not in git)
    if File.exist?('ext/librdkafka.so')
      gem.files << 'ext/librdkafka.so'
    end

    if File.exist?('ext/librdkafka.dylib')
      gem.files << 'ext/librdkafka.dylib'
    end
  else
    gem.platform = Gem::Platform::RUBY
    gem.files = `git ls-files`.split($\)
    gem.extensions = %w(ext/Rakefile)
  end

  gem.add_dependency 'ffi', '~> 1.15'
  gem.add_dependency 'mini_portile2', '~> 2.6'
  gem.add_dependency 'rake', '> 12'

  gem.add_development_dependency 'pry'
  gem.add_development_dependency 'rspec', '~> 3.5'
  gem.add_development_dependency 'rake'
  gem.add_development_dependency 'simplecov'

  gem.metadata = {
    'funding_uri' => 'https://karafka.io/#become-pro',
    'homepage_uri' => 'https://karafka.io',
    'changelog_uri' => 'https://karafka.io/docs/Changelog-Karafka-Rdkafka/',
    'bug_tracker_uri' => 'https://github.com/karafka/karafka-rdkafka/issues',
    'source_code_uri' => 'https://github.com/karafka/karafka-rdkafka',
    'documentation_uri' => 'https://karafka.io/docs',
    'rubygems_mfa_required' => 'true'
  }
end
