# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/jdbc"
require "yaml" # persistence

# This plugin was created as a way to iteratively ingest data in any database
# with a JDBC interface into Logstash. You can periodically schedule ingestion
# using a cron syntax (see `schedule` setting) or run the query one time to load
# data into Logstash. Each row in the resultset becomes a single event.
# Columns in the resultset are converted into fields in the event.
#
# ==== Sample usage:
#
# This is an example logstash config
# [source,ruby]
# ----------------------------------
# input {
#   jdbc {
#     jdbc_driver_class => "org.apache.derby.jdbc.EmbeddedDriver" (required)
#     jdbc_connection_string => "jdbc:derby:memory:testdb;create=true" (required)
#     jdbc_user => "username" (required)
#     jdbc_password => "mypass"
#     statement => "SELECT * from table where created_at > :sql_last_start and id = :my_id" (required)
#     parameters => { "my_id" => "231" }
#     schedule => "* * * * *"
#   }
# }
# ----------------------------------
#
# ==== Configuring SQL statement
# 
# A sql statement is required for this input. This can be passed-in via a 
# statement option in the form of a string, or read from a file (`statement_filepath`). File 
# option is typically used when the SQL statement is large or cumbersome to supply in the config.
# The file option only supports one SQL statement. The plugin will only accept one of the options.
# It cannot read a statement from a file as well as from the `statement` configuration parameter.
#
# ==== Predefined Parameters
#
# Some parameters are built-in and can be used from within your queries.
# Here is the list:
#
# |==========================================================
# |sql_last_start |The time the last query executed in plugin
# |==========================================================
#
class LogStash::Inputs::Jdbc < LogStash::Inputs::Base
  include LogStash::PluginMixins::Jdbc
  config_name "jdbc"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain" 

  # Statement to execute
  #
  # To use parameters, use named parameter syntax.
  # For example:
  #
  # [source, ruby]
  # ----------------------------------
  # "SELECT * FROM MYTABLE WHERE id = :target_id"
  # ----------------------------------
  #
  # here, ":target_id" is a named parameter. You can configure named parameters
  # with the `parameters` setting.
  config :statement, :validate => :string

  # Path of file containing statement to execute
  config :statement_filepath, :validate => :path

  # Hash of query parameter, for example `{ "target_id" => "321" }`
  config :parameters, :validate => :hash, :default => {}

  # Schedule of when to periodically run statement, in Cron format
  # for example: "* * * * *" (execute query every minute, on the minute)
  #
  # There is no schedule by default. If no schedule is given, then the statement is run
  # exactly once.
  config :schedule, :validate => :string

  # Path to file with last run time
  config :last_run_metadata_path, :validate => :string, :default => "#{ENV['HOME']}/.logstash_jdbc_last_run"

  # Whether the previous run state should be preserved
  config :clean_run, :validate => :boolean, :default => false

  # Whether to save state or not in last_run_metadata_path
  config :record_last_run, :validate => :boolean, :default => true

  public

  def register
    require "rufus/scheduler"
    prepare_jdbc_connection

    # load sql_last_start from file if exists
    if @clean_run && File.exist?(@last_run_metadata_path)
      File.delete(@last_run_metadata_path)
    elsif File.exist?(@last_run_metadata_path)
      @sql_last_start = YAML.load(File.read(@last_run_metadata_path))
    end

    unless @statement.nil? ^ @statement_filepath.nil?
      raise(LogStash::ConfigurationError, "Must set either :statement or :statement_filepath. Only one may be set at a time.")
    end

    @statement = File.read(@statement_filepath) if @statement_filepath
  end # def register

  def run(queue)
    if @schedule
      @scheduler = Rufus::Scheduler.new
      @scheduler.cron @schedule do
        execute_query(queue)
      end
      @scheduler.join
    else
      execute_query(queue)
    end
  end # def run

  def teardown
    @scheduler.stop if @scheduler

    # update state file for next run
    if @record_last_run
      File.write(@last_run_metadata_path, YAML.dump(@sql_last_start))
    end

    close_jdbc_connection
  end # def teardown

  private

  def execute_query(queue)
    # update default parameters
    @parameters['sql_last_start'] = @sql_last_start
    execute_statement(@statement, @parameters) do |row|
      event = LogStash::Event.new(row)
      decorate(event)
      queue << event
    end
  end
end # class LogStash::Inputs::Jdbc
