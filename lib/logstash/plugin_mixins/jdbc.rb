# encoding: utf-8
# TAKEN FROM WIIBAA
require "logstash/config/mixin"

# Tentative of abstracting JDBC logic to a mixin 
# for potential reuse in other plugins (input/output)
module LogStash::PluginMixins::Jdbc

  @logger = Cabin::Channel.get(LogStash)

  # This method is called when someone includes this module
  def self.included(base)
    # Add these methods to the 'base' given.
    base.extend(self)
    base.setup_jdbc_config

    @sql_last_start = Time.at(0).utc
  end


  public
  def setup_jdbc_config
    # JDBC driver library path to third party driver library.
    config :jdbc_driver_library, :validate => :path

    # JDBC driver class to load, for example "oracle.jdbc.OracleDriver" or "org.apache.derby.jdbc.ClientDriver"
    config :jdbc_driver_class, :validate => :string, :required => true

    # JDBC connection string
    config :jdbc_connection_string, :validate => :string, :required => true

    # JDBC user
    config :jdbc_user, :validate => :string, :default => "tal"

    # JDBC password
    config :jdbc_password, :validate => :password

    # Connection pool configuration.
    # Validate connection before use.
    config :jdbc_validate_connection, :validate => :boolean, :default => false

    # Connection pool configuration.
    # How often to validate a connection (in seconds)
    config :jdcb_validation_timeout, :validate => :number, :default => 3600
  end

  public
  def prepare_jdbc_connection
    require "java"
    require "sequel"
    require "sequel/adapters/jdbc"
    require @jdbc_driver_library if @jdbc_driver_library
    Sequel::JDBC.load_driver(@jdbc_driver_class)
    @database = Sequel.connect(@jdbc_connection_string, :user=> @jdbc_user, :password=>  @jdbc_password.nil? ? nil : @jdbc_password.value)
    if @jdbc_validate_connection
      @database.extension(:connection_validator)
      @database.pool.connection_validation_timeout = @jdcb_validation_timeout
    end
    begin
      @database.test_connection
    rescue Sequel::DatabaseConnectionError => e
      #TODO return false and let the plugin raise a LogStash::ConfigurationError
      raise e
    end
  end # def prepare_jdbc_connection

  public
  def close_jdbc_connection
    @database.disconnect if @database
  end

  public
  def execute_statement(statement, parameters)
    success = false
    begin 
      parameters = symbolized_params(parameters)
      query = @database[statement, parameters]
      @logger.debug? and @logger.debug("Executing JDBC query", :statement => statement, :parameters => parameters)
      @sql_last_start = Time.now.utc
      query.all do |row|
        #Stringify row keys
        yield Hash[row.map { |k, v| [k.to_s, v] }]
      end
      success = true
    rescue Sequel::DatabaseConnectionError, Sequel::DatabaseError => e
      @logger.warn("Exception when executing JDBC query", :exception => e)
    end
    return success
  end

  # Symbolize parameters keys to use with Sequel
  private 
  def symbolized_params(parameters)
    parameters.inject({}) do |hash,(k,v)| 
      case v
      when LogStash::Timestamp
        hash[k.to_sym] = v.time
      else
        hash[k.to_sym] = v
      end
      hash
    end
  end
end
