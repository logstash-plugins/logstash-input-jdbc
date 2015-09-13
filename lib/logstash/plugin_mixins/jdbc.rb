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
  end

  public
  def prepare_jdbc_connection(jdbcConnection)
    require "java"
    require "sequel"
    require "sequel/adapters/jdbc"
    require jdbcConnection.jdbc_driver_library if jdbcConnection.jdbc_driver_library

    Sequel::JDBC.load_driver(jdbcConnection.jdbc_driver_class)
    @database = Sequel.connect(jdbcConnection.jdbc_connection_string, :user=> jdbcConnection.jdbc_user, :password=>  jdbcConnection.jdbc_password.nil? ? nil : jdbcConnection.jdbc_password)
    @database.extension(:pagination)

    if jdbcConnection.jdbc_validate_connection
      @database.extension(:connection_validator)
      @database.pool.connection_validation_timeout = jdbcConnection.jdbc_validation_timeout
    end

    @database.fetch_size = jdbcConnection.jdbc_fetch_size unless jdbcConnection.jdbc_fetch_size.nil?
    begin
      @database.test_connection
    rescue Sequel::DatabaseConnectionError => e
      #TODO return false and let the plugin raise a LogStash::ConfigurationError
      raise e
    end

    #@sql_last_start = Time.at(0).utc
  end # def prepare_jdbc_connection

  public
  def close_jdbc_connection
    @database.disconnect if @database
  end

  public
  def execute_statement(jdbcConnection, statement, parameters)
    success = false
    begin
      parameters = symbolized_params(parameters)
      query = @database[statement, parameters]
      @logger.debug? and @logger.debug("Executing JDBC query", :statement => statement, :parameters => parameters)
      #@sql_last_start = Time.now.utc

      if jdbcConnection.jdbc_paging_enabled
        query.each_page(jdbcConnection.jdbc_page_size) do |paged_dataset|
          paged_dataset.each do |row|
            #Stringify row keys
            yield Hash[row.map { |k, v| [k.to_s, v] }]
          end
        end
      else
        query.each do |row|
          #Stringify row keys
          yield Hash[row.map { |k, v| [k.to_s, v] }]
        end
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
