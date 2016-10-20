## 4.1.3
 - Fix part1 of #172, coerce SQL DATE to LS Timestamp

## 4.1.2
 - [internal] Removed docker dependencies for testing

## 4.1.1
 - Relax constraint on logstash-core-plugin-api to >= 1.60 <= 2.99

## 4.1.0
 - Add an option to select the encoding data should be transform from,
   this will make sure all strings read from the jdbc connector are
   noremalized to be UTF-8 so no causing issues with later filters in LS.

## 4.0.1
 - Republish all the gems under jruby.

## 4.0.0
 - Update the plugin to the version 2.0 of the plugin api, this change is required for Logstash 5.0 compatibility. See https://github.com/elastic/logstash/issues/5141

# 3.0.3
 - Added feature to read password from external file (#120)

# 3.0.2
 - Depend on logstash-core-plugin-api instead of logstash-core, removing the need to mass update plugins on major releases of logstash

# 3.0.1
 - New dependency requirements for logstash-core for the 5.0 release
 - feature: Added configurable support for retrying database connection
   failures.

## 3.0.0
 - [#57](https://github.com/logstash-plugins/logstash-input-jdbc/issues/57) New feature: Allow tracking by a column value rather than by last run time.  **This is a breaking change**, as users may be required to change from using `sql_last_start` to use `sql_last_value` in their queries.  No other changes are required if you've been using time-based queries.  See the documentation if you wish to use an incremental column value to track updates to your tables.

## 2.1.1
 - [#44](https://github.com/logstash-plugins/logstash-input-jdbc/issues/44) add option to control the lowercase or not, of the column names.

## 2.1.0
 - [#85](https://github.com/logstash-plugins/logstash-input-jdbc/issues/85) make the jdbc_driver_library accept a list of elements separated by commas as in some situations we might need to load more than one jar/lib.
 - [#89](https://github.com/logstash-plugins/logstash-input-jdbc/issues/89) Set application timezone for cases where time fields in data have no timezone.

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

## 1.0.0
 - Initial release
