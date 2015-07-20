# encoding: utf-8

class LogStash::PluginMixins::JdbcConnection

  @logger = Cabin::Channel.get(LogStash)

  # JDBC driver library path to third party driver library.
  attr_accessor :jdbc_driver_library

  # JDBC driver class to load, for example "oracle.jdbc.OracleDriver" or "org.apache.derby.jdbc.ClientDriver"
  attr_accessor :jdbc_driver_class

  # JDBC connection string
  attr_accessor :jdbc_connection_string

  # JDBC user
  attr_accessor :jdbc_user

  # JDBC password
  attr_accessor :jdbc_password

  # JDBC enable paging
  #
  # This will cause a sql statement to be broken up into multiple queries.
  # Each query will use limits and offsets to collectively retrieve the full
  # result-set. The limit size is set with `jdbc_page_size`.
  #
  # Be aware that ordering is not guaranteed between queries.
  attr_accessor :jdbc_paging_enabled

  # JDBC page size
  attr_accessor :jdbc_page_size

  # JDBC fetch size. if not provided, respective driver's default will be used
  attr_accessor :jdbc_fetch_size

  # Connection pool configuration.
  # Validate connection before use.
  attr_accessor :jdbc_validate_connection

  # Connection pool configuration.
  # How often to validate a connection (in seconds)
  attr_accessor :jdbc_validation_timeout

  def populate(connectionConfig)

    @jdbc_driver_library = connectionConfig['jdbc_driver_library']
    @jdbc_driver_class = connectionConfig['jdbc_driver_class']
    @jdbc_connection_string = connectionConfig['jdbc_connection_string']
    @jdbc_user = connectionConfig['jdbc_user']
    @jdbc_password = connectionConfig['jdbc_password']

    if connectionConfig['jdbc_paging_enabled'].nil?
      @jdbc_paging_enabled = false
    else
      @jdbc_paging_enabled = connectionConfig['jdbc_paging_enabled']
    end

    if connectionConfig['jdbc_page_size'].nil?
      @jdbc_page_size = 100000
    else
      @jdbc_page_size = connectionConfig['jdbc_page_size']
    end

    @jdbc_fetch_size = connectionConfig['jdbc_fetch_size']

    if connectionConfig['jdbc_validate_connection'].nil?
      @jdbc_validate_connection = false
    else
      @jdbc_validate_connection = connectionConfig['jdbc_validate_connection']
    end

    if connectionConfig['jdbc_validation_timeout'].nil?
      @jdbc_validation_timeout = 3600
    else
      @jdbc_validation_timeout = connectionConfig['jdbc_validation_timeout']
    end

    if validate == false
      raise 'Error'
    end

  end

  def validate

    false if :jdbc_connection_string.nil? || :jdbc_driver_class.nil? || :jdbc_connection_string.nil? || :jdbc_user.nil?
    true

  end

  def printObject

    puts "JdbcConnection => {"
    puts "    jdbc_driver_library      = #{jdbc_driver_library}"
    puts "    jdbc_driver_class        = #{jdbc_driver_class}"
    puts "    jdbc_connection_string   = #{jdbc_connection_string}"
    puts "    jdbc_user                = #{jdbc_user}"
    puts "    jdbc_password            = #{jdbc_password}"
    puts "    jdbc_paging_enabled      = #{jdbc_paging_enabled}"
    puts "    jdbc_page_size           = #{jdbc_page_size}"
    puts "    jdbc_fetch_size          = #{jdbc_fetch_size}"
    puts "    jdbc_validate_connection = #{jdbc_validate_connection}"
    puts "    jdbc_validation_timeout  = #{jdbc_validation_timeout}"
    puts "}"

  end

end
