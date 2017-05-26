source 'https://rubygems.org'

gemspec

logstash_path = "../../logstash"

if Dir.exist?(logstash_path) && ENV["LOGSTASH_SOURCE"] == 1
  gem 'logstash-core', :path => "#{logstash_path}/logstash-core"
  gem 'logstash-core-plugin-api', :path => "#{logstash_path}/logstash-core-plugin-api"
end
