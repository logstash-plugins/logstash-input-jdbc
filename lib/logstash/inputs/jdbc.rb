# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/jdbc"

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

  public

  def register
    require "rufus/scheduler"
    prepare_jdbc_connection()
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
