require File.expand_path('../lib/rdkafka/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors = ['Thijs Cadier']
  gem.email = ["thijs@appsignal.com"]
  gem.description = "Modern Kafka client library for Ruby based on librdkafka"
  gem.summary = "Kafka client library wrapping librdkafka using the ffi gem and futures from concurrent-ruby for Kafka 0.10+"
  gem.license = 'MIT'

  gem.files = `git ls-files`.split($\)
  gem.executables = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files = gem.files.grep(%r{^(test|spec|features)/})
  gem.name = 'rdkafka'
  gem.require_paths = ['lib']
  gem.version = Rdkafka::VERSION
  gem.required_ruby_version = '>= 2.0'
  gem.extensions = %w(ext/Rakefile)

  gem.add_dependency 'ffi'
  gem.add_dependency 'mini_portile2'

  gem.add_development_dependency 'pry'
  gem.add_development_dependency 'rspec', '~> 3.5'
  gem.add_development_dependency 'rake'
end
