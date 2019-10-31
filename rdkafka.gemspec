require File.expand_path('lib/rdkafka/version', __dir__)

Gem::Specification.new do |gem|
  gem.authors = ['Thijs Cadier']
  gem.email = ["thijs@appsignal.com"]
  gem.description = "Modern Kafka client library for Ruby based on librdkafka"
  gem.summary = "The rdkafka gem is a modern Kafka client library for Ruby based on librdkafka. It wraps the production-ready C client using the ffi gem and targets Kafka 1.0+ and Ruby 2.4+."
  gem.license = 'MIT'
  gem.homepage = 'https://github.com/thijsc/rdkafka-ruby'

  gem.files = `git ls-files`.split($\)
  gem.executables = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files = gem.files.grep(%r{^(test|spec|features)/})
  gem.name = 'rdkafka'
  gem.require_paths = ['lib']
  gem.version = Rdkafka::VERSION
  gem.required_ruby_version = '>= 2.4'
  gem.extensions = %w(ext/Rakefile)

  gem.add_dependency 'ffi', '~> 1.9'
  gem.add_dependency 'mini_portile2', '~> 2.1'
  gem.add_dependency 'rake', '>= 12.3'

  gem.add_development_dependency 'pry', '~> 0.10'
  gem.add_development_dependency 'rspec', '~> 3.5'
  gem.add_development_dependency 'rake', '~> 12.0'
  gem.add_development_dependency 'simplecov', '~> 0.15'
end
