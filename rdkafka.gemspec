# frozen_string_literal: true

require File.expand_path('lib/rdkafka/version', __dir__)

Gem::Specification.new do |gem|
  gem.authors = ['Thijs Cadier', 'Maciej Mensfeld']
  gem.email = ["contact@karafka.io"]
  gem.description = "Modern Kafka client library for Ruby based on librdkafka"
  gem.summary = "The rdkafka gem is a modern Kafka client library for Ruby based on librdkafka. It wraps the production-ready C client using the ffi gem and targets Kafka 1.0+ and Ruby 2.7+."
  gem.license = 'MIT'

  gem.test_files = gem.files.grep(%r{^(test|spec|features)/})
  gem.name = 'rdkafka'
  gem.require_paths = ['lib']
  gem.version = Rdkafka::VERSION
  gem.required_ruby_version = '>= 3.2'

  files = `git ls-files`.split($\)
  files = files.reject do |file|
    next true if file.start_with?('.')
    next true if file.start_with?('spec/')
    next true if file.start_with?('ext/README.md')

    false
  end

  if ENV['RUBY_PLATFORM']
    gem.platform = ENV['RUBY_PLATFORM']

    # Do not include the source code for librdkafka as it should be precompiled already per
    # platform. Same applies to any possible patches.
    # Do not include github actions details in RubyGems releases
    gem.files = files.reject do |file|
      next true if file.start_with?('dist/')
      next true if file.start_with?('ext/build_')
      next true if file.start_with?('ext/ci_')
      next true if file.start_with?('ext/Rakefile')
      next true if file.start_with?('ext/generate-')

      false
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

    # Do not include code used for building native extensions
    # Do not include github actions details in RubyGems releases
    gem.files = files.reject do |file|
      next true if file.start_with?('ext/build_')
      next true if file.start_with?('ext/ci_')
      next true if file.start_with?('ext/generate-')
      next false unless file.start_with?('dist/')
      next false if file.start_with?('dist/patches')
      next false if file.start_with?('dist/librdkafka-')

      true
    end

    gem.extensions = %w(ext/Rakefile)
  end

  gem.add_dependency 'ffi', '~> 1.15'
  gem.add_dependency 'json', '> 2.0'
  gem.add_dependency 'logger'
  gem.add_dependency 'mini_portile2', '~> 2.6'
  gem.add_dependency 'rake', '> 12'

  gem.add_development_dependency 'ostruct'
  gem.add_development_dependency 'pry'
  gem.add_development_dependency 'rspec', '~> 3.5'
  gem.add_development_dependency 'rake'
  gem.add_development_dependency 'simplecov'
  gem.add_development_dependency 'warning'

  gem.metadata = {
    'funding_uri' => 'https://karafka.io/#become-pro',
    'homepage_uri' => 'https://karafka.io',
    'changelog_uri' => 'https://karafka.io/docs/Changelog-Rdkafka',
    'bug_tracker_uri' => 'https://github.com/karafka/rdkafka-ruby/issues',
    'source_code_uri' => 'https://github.com/karafka/rdkafka-ruby',
    'documentation_uri' => 'https://karafka.io/docs',
    'rubygems_mfa_required' => 'true'
  }
end
