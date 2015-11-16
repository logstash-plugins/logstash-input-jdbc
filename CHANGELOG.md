## 2.0.5
  - [#77](https://github.com/logstash-plugins/logstash-input-jdbc/issues/77) Time represented as RubyTime and not as Logstash::Timestamp

## 2.0.4
  - [#70](https://github.com/logstash-plugins/logstash-input-jdbc/pull/70) prevents multiple queries from being run at the same time
  - [#69](https://github.com/logstash-plugins/logstash-input-jdbc/pull/69) pass password as string to Sequel

## 2.0.3
 - Added ability to configure timeout
 - Added catch-all configuration option for any other options that Sequel lib supports

## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully, 
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

* 1.0.0
  - Initial release
