# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/jdbc"
require "yaml" # persistence

# INFORMATION
#
# This plugin was created as a way to iteratively ingest any database
# with a JDBC interface into Logstash.
#
# #### JDBC Mixin
#
# This plugin utilizes a mixin that helps Logstash plugins manage JDBC connections.
# The mixin provides its own set of configurations (some are required) to properly 
# set up the connection to the appropriate database.
#
# #### Predefined Parameters
#
# Some parameters are built-in and can be used from within your queries.
# Here is the list:
#
# |==========================================================
# |sql_last_start |The time the last query executed in plugin
# |==========================================================
#
# #### Usage:
# This is an example logstash config
# [source,ruby]
# input {
#   jdbc {
#     jdbc_driver_class => "org.apache.derby.jdbc.EmbeddedDriver" (required; from mixin)
#     jdbc_connection_string => "jdbc:derby:memory:testdb;create=true" (required; from mixin)
#     jdbc_user => "username" (from mixin)
#     jdbc_password => "mypass" (from mixin)
#     statement => "SELECT * from table where created_at > :sql_last_start and id = :my_id" (required)
#     parameters => { "my_id" => "231" }
#     schedule => "* * * * *"
#   }
# }
class LogStash::Inputs::Jdbc < LogStash::Inputs::Base
  include LogStash::PluginMixins::Jdbc
  config_name "jdbc"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain" 

  # Statement to execute
  # To use parameters, use named parameter syntax.
  # For example:
  # "SELECT * FROM MYTABLE WHERE id = :target_id"
  # here ":target_id" is a named parameter
  #
  config :statement, :validate => :string, :required => true

  # Hash of query parameter, for example `{ "target_id" => "321" }`
  config :parameters, :validate => :hash, :default => {}

  # Schedule of when to periodically run statement, in Cron format
  # for example: "* * * * *" (execute query every minute, on the minute)
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
    prepare_jdbc_connection()

    # load sql_last_start from file if exists
    if @clean_run && File.exists?(@last_run_metadata_path)
      File.delete(@last_run_metadata_path)
    elsif File.exists?(@last_run_metadata_path)
      @sql_last_start = YAML.load(File.read(@last_run_metadata_path))
    end
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
    if @scheduler
      @scheduler.stop
    end

    # update state file for next run
    if @record_last_run
      File.open(@last_run_metadata_path, 'w') do |f|
        f.write(YAML.dump(@sql_last_start))
      end
    end

    close_jdbc_connection()
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
