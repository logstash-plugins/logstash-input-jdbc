Gem::Specification.new do |s|
  s.name = 'logstash-input-jdbc'
  s.version         = '0.1.1'
  s.licenses = ['Apache License (2.0)']
  s.summary = "This example input streams a string at a definable interval."
  s.description = "This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/plugin install gemname. This gem is not a stand-alone program"
  s.authors = ["Elastic"]
  s.email = 'info@elastic.co'
  s.homepage = "http://www.elastic.co/guide/en/logstash/current/index.html"
  s.require_paths = ["lib"]

  # Files
  s.files = `git ls-files`.split($\)
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "input" }

  # Gem dependencies
  s.add_runtime_dependency 'logstash-core', '>= 1.4.0', '< 2.0.0'
  s.add_runtime_dependency 'logstash-codec-plain'
  s.add_runtime_dependency 'sequel'

  s.add_development_dependency 'logstash-devutils'
  s.add_development_dependency 'timecop'
  s.add_development_dependency 'rufus-scheduler'
  s.add_development_dependency 'jdbc-derby'
end
