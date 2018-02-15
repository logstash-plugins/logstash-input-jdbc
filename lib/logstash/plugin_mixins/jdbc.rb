# encoding: utf-8
# TAKEN FROM WIIBAA
require "logstash/config/mixin"
require "time"
require "date"
require "logstash/plugin_mixins/value_tracking"

java_import java.util.concurrent.locks.ReentrantLock

# Tentative of abstracting JDBC logic to a mixin
# for potential reuse in other plugins (input/output)
module LogStash::PluginMixins::Jdbc

  # This method is called when someone includes this module
  def self.included(base)
    # Add these methods to the 'base' given.
    base.extend(self)
    base.setup_jdbc_config
  end


  public
  def setup_jdbc_config
    # JDBC driver library path to third party driver library. In case of multiple libraries being
    # required you can pass them separated by a comma.
    #
    # If not provided, Plugin will look for the driver class in the Logstash Java classpath.
    config :jdbc_driver_library, :validate => :string

    # JDBC driver class to load, for exmaple, "org.apache.derby.jdbc.ClientDriver"
    # NB per https://github.com/logstash-plugins/logstash-input-jdbc/issues/43 if you are using
    # the Oracle JDBC driver (ojdbc6.jar) the correct `jdbc_driver_class` is `"Java::oracle.jdbc.driver.OracleDriver"`
    config :jdbc_driver_class, :validate => :string, :required => true

    # JDBC connection string
    config :jdbc_connection_string, :validate => :string, :required => true

    # JDBC user
    config :jdbc_user, :validate => :string, :required => true

    # JDBC password
    config :jdbc_password, :validate => :password

    # JDBC password filename
    config :jdbc_password_filepath, :validate => :path

    # JDBC enable paging
    #
    # This will cause a sql statement to be broken up into multiple queries.
    # Each query will use limits and offsets to collectively retrieve the full
    # result-set. The limit size is set with `jdbc_page_size`.
    #
    # Be aware that ordering is not guaranteed between queries.
    config :jdbc_paging_enabled, :validate => :boolean, :default => false

    # JDBC page size
    config :jdbc_page_size, :validate => :number, :default => 100000

    # JDBC fetch size. if not provided, respective driver's default will be used
    config :jdbc_fetch_size, :validate => :number

    # Connection pool configuration.
    # Validate connection before use.
    config :jdbc_validate_connection, :validate => :boolean, :default => false

    # Connection pool configuration.
    # How often to validate a connection (in seconds)
    config :jdbc_validation_timeout, :validate => :number, :default => 3600

    # Connection pool configuration.
    # The amount of seconds to wait to acquire a connection before raising a PoolTimeoutError (default 5)
    config :jdbc_pool_timeout, :validate => :number, :default => 5

    # Timezone conversion.
    # SQL does not allow for timezone data in timestamp fields.  This plugin will automatically
    # convert your SQL timestamp fields to Logstash timestamps, in relative UTC time in ISO8601 format.
    #
    # Using this setting will manually assign a specified timezone offset, instead
    # of using the timezone setting of the local machine.  You must use a canonical
    # timezone, *America/Denver*, for example.
    config :jdbc_default_timezone, :validate => :string

    # General/Vendor-specific Sequel configuration options.
    #
    # An example of an optional connection pool configuration
    #    max_connections - The maximum number of connections the connection pool
    #
    # examples of vendor-specific options can be found in this
    # documentation page: https://github.com/jeremyevans/sequel/blob/master/doc/opening_databases.rdoc
    config :sequel_opts, :validate => :hash, :default => {}

    # Log level at which to log SQL queries, the accepted values are the common ones fatal, error, warn,
    # info and debug. The default value is info.
    config :sql_log_level, :validate => [ "fatal", "error", "warn", "info", "debug" ], :default => "info"

    # Maximum number of times to try connecting to database
    config :connection_retry_attempts, :validate => :number, :default => 1
    # Number of seconds to sleep between connection attempts
    config :connection_retry_attempts_wait_time, :validate => :number, :default => 0.5
  end

  private
  def jdbc_connect
    opts = {
      :user => @jdbc_user,
      :password => @jdbc_password.nil? ? nil : @jdbc_password.value,
      :pool_timeout => @jdbc_pool_timeout,
      :keep_reference => false
    }.merge(@sequel_opts)
    retry_attempts = @connection_retry_attempts
    loop do
      retry_attempts -= 1
      begin
        return Sequel.connect(@jdbc_connection_string, opts=opts)
      rescue Sequel::PoolTimeout => e
        if retry_attempts <= 0
          @logger.error("Failed to connect to database. #{@jdbc_pool_timeout} second timeout exceeded. Tried #{@connection_retry_attempts} times.")
          raise e
        else
          @logger.error("Failed to connect to database. #{@jdbc_pool_timeout} second timeout exceeded. Trying again.")  
        end
      rescue Sequel::Error => e
        if retry_attempts <= 0
          @logger.error("Unable to connect to database. Tried #{@connection_retry_attempts} times", :error_message => e.message, )
          raise e
        else
          @logger.error("Unable to connect to database. Trying again", :error_message => e.message)
        end
      end
      sleep(@connection_retry_attempts_wait_time)
    end
  end

  private
  def load_drivers(drivers)
    drivers.each do |driver|
      begin
        require driver
      rescue => e
        @logger.error("Failed to load #{driver}", :exception => e)
      end
    end
  end

  private
  def open_jdbc_connection
    require "java"
    require "sequel"
    require "sequel/adapters/jdbc"
    load_drivers(@jdbc_driver_library.split(",")) if @jdbc_driver_library

    begin
      Sequel::JDBC.load_driver(@jdbc_driver_class)
    rescue Sequel::AdapterNotFound => e
      message = if @jdbc_driver_library.nil?
                  ":jdbc_driver_library is not set, are you sure you included
                  the proper driver client libraries in your classpath?"
                else
                  "Are you sure you've included the correct jdbc driver in :jdbc_driver_library?"
                end
      raise LogStash::ConfigurationError, "#{e}. #{message}"
    end
    @database = jdbc_connect()
    @database.extension(:pagination)
    if @jdbc_default_timezone
      @database.extension(:named_timezones)
      @database.timezone = @jdbc_default_timezone
    end
    if @jdbc_validate_connection
      @database.extension(:connection_validator)
      @database.pool.connection_validation_timeout = @jdbc_validation_timeout
    end
    @database.fetch_size = @jdbc_fetch_size unless @jdbc_fetch_size.nil?
    begin
      @database.test_connection
    rescue Sequel::DatabaseConnectionError => e
      @logger.warn("Failed test_connection.", :exception => e)
      close_jdbc_connection

      #TODO return false and let the plugin raise a LogStash::ConfigurationError
      raise e
    end

    @database.sql_log_level = @sql_log_level.to_sym
    @database.logger = @logger

    @database.extension :identifier_mangling

    if @lowercase_column_names
      @database.identifier_output_method = :downcase
    else
      @database.identifier_output_method = :to_s
    end
  end

  public
  def prepare_jdbc_connection
    @connection_lock = ReentrantLock.new
  end

  public
  def close_jdbc_connection
    begin
      # pipeline restarts can also close the jdbc connection, block until the current executing statement is finished to avoid leaking connections
      # connections in use won't really get closed
      @connection_lock.lock
      @database.disconnect if @database
    rescue => e
      @logger.warn("Failed to close connection", :exception => e)
    ensure
      @connection_lock.unlock
    end
  end

  public
  def execute_statement(statement, parameters)
      success = false
      @connection_lock.lock
      open_jdbc_connection
      begin
        parameters = symbolized_params(parameters)
        query = @database[statement, parameters]

        sql_last_value = @use_column_value ? @value_tracker.value : Time.now.utc
        @tracking_column_warning_sent = false
        @logger.debug? and @logger.debug("Executing JDBC query", :statement => statement, :parameters => parameters, :count => query.count)

        perform_query(query) do |row|
          sql_last_value = get_column_value(row) if @use_column_value
          yield extract_values_from(row)
        end
        success = true
      rescue Sequel::DatabaseConnectionError, Sequel::DatabaseError => e
        @logger.warn("Exception when executing JDBC query", :exception => e)
      else
        @value_tracker.set_value(sql_last_value)
      ensure
        close_jdbc_connection
        @connection_lock.unlock
      end
      return success
  end

  # Performs the query, respecting our pagination settings, yielding once per row of data
  # @param query [Sequel::Dataset]
  # @yieldparam row [Hash{Symbol=>Object}]
  private
  def perform_query(query)
    if @jdbc_paging_enabled
      query.each_page(@jdbc_page_size) do |paged_dataset|
        paged_dataset.each do |row|
          yield row
        end
      end
    else
      query.each do |row|
        yield row
      end
    end
  end

  public
  def get_column_value(row)
    if !row.has_key?(@tracking_column.to_sym)
      if !@tracking_column_warning_sent
        @logger.warn("tracking_column not found in dataset.", :tracking_column => @tracking_column)
        @tracking_column_warning_sent = true
      end
      # If we can't find the tracking column, return the current value in the ivar
      @sql_last_value
    else
      # Otherwise send the updated tracking column
      row[@tracking_column.to_sym]
    end
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

  private
  #Stringify row keys and decorate values when necessary
  def extract_values_from(row)
    Hash[row.map { |k, v| [k.to_s, decorate_value(v)] }]
  end

  private
  def decorate_value(value)
    if value.is_a?(Time)
      # transform it to LogStash::Timestamp as required by LS
      LogStash::Timestamp.new(value)
    elsif value.is_a?(Date)
      LogStash::Timestamp.new(value.to_time)
    elsif value.is_a?(DateTime)
      # Manual timezone conversion detected.
      # This is slower, so we put it in as a conditional case.
      LogStash::Timestamp.new(Time.parse(value.to_s))
    else
      value
    end
  end
end
