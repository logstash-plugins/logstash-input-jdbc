class LogStash::Inputs::Statement

  @logger = Cabin::Channel.get(LogStash)

  attr_accessor :query
  attr_accessor :query_filepath
  attr_accessor :parameters
  attr_accessor :schedule
  attr_accessor :join_keys
  attr_accessor :node_name
  attr_accessor :persistence_data
  attr_accessor :persistence_store_type
  attr_accessor :file
  attr_accessor :elastic_search
  attr_accessor :clear_persistence_store
  attr_accessor :statement

  def populate(stmt)

    @query = stmt['query']
    @query_filepath = File.read(stmt['statement_filepath']) if stmt['statement_filepath']
    @parameters = stmt['parameters']
    @schedule = stmt['schedule']
    @join_keys = stmt['join_keys']
    @node_name = stmt['node_name']
    @persistence_data = stmt['persistence_data']
    @persistence_store_type = stmt['persistence_store_type']
    @file = stmt['file']
    @elastic_search = stmt['elastic_search']

    if stmt['clear_persistence_store'].nil?
      @clear_persistence_store = false
    else
      @clear_persistence_store = stmt['clear_persistence_store']
    end

    if !stmt['statement'].nil?
      @statement = LogStash::Inputs::Statement.new()
      @statement.populate(stmt['statement'])
    end

  end

  def validate

    unless @query.nil? ^ @query_filepath.nil?
      raise(LogStash::ConfigurationError, "Must set either :query or :query_filepath. Only one may be set at a time.")
      false
    end

    true
  end

  def printObject (level=0)
    puts ' '*(4*level) + 'Statement => {'
    puts ' '*(4*level) + "    query                   = #{query}"
    puts ' '*(4*level) + "    query_filepath          = #{query_filepath}"
    puts ' '*(4*level) + "    parameters              = #{parameters}"
    puts ' '*(4*level) + "    schedule                = #{schedule}"
    puts ' '*(4*level) + "    join_keys               = #{join_keys}"
    puts ' '*(4*level) + "    node_name               = #{node_name}"
    puts ' '*(4*level) + "    persistence_data        = #{persistence_data}"
    puts ' '*(4*level) + "    persistence_store_type  = #{persistence_store_type}"
    puts ' '*(4*level) + "    file                    = #{file}"
    puts ' '*(4*level) + "    elastic_search          = #{elastic_search}"
    puts ' '*(4*level) + "    clear_persistence_store = #{clear_persistence_store}"
    @statement.printObject(level+1) if !@statement.nil?
    puts ' '*(4*level) + '}'

  end


end