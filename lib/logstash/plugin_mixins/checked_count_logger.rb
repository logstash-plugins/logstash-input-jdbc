
module LogStash module PluginMixins
  class CheckedCountLogger
    def initialize(logger)
      @logger = logger
      @needs_check = true
      @count_is_supported = false
      @in_debug = @logger.debug?
    end

    def log_statement_parameters(query, statement, parameters)
      return unless @in_debug
      check_count_query(query) if @needs_check
      if @count_is_supported
        @logger.debug("Executing JDBC query", :statement => statement, :parameters => parameters, :count => query.count)
      else
        @logger.debug("Executing JDBC query", :statement => statement, :parameters => parameters)
      end
    end

    def check_count_query(query)
      begin
        execute_count(query)
        @count_is_supported = true
      rescue Exception => e
        @logger.info("Disabling count queries as the executing the count SQL raised an error")
      end
      @needs_check = false
    end

    def execute_count(query)
      query.count
    end
  end
end end