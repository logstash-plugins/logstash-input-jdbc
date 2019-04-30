# encoding: utf-8
require "logstash/util/loggable"

module LogStash module PluginMixins module Jdbc
  class StatementHandler
    include LogStash::Util::Loggable
    def self.build_statement_handler(plugin)
      klass = plugin.use_prepared_statements ? PreparedStatementHandler: NormalStatementHandler
      klass.new(plugin)
    end

    attr_reader :statement, :parameters

    def initialize(plugin)
      @statement = plugin.statement
      post_init(plugin)
    end

    def build_query(db, sql_last_value)
      # override in subclass
    end

    def post_init(plugin)
      # override in subclass, if needed
    end
  end

  class NormalStatementHandler < StatementHandler
    def build_query(db, sql_last_value)
      parameters[:sql_last_value] = sql_last_value
      query = db[statement, parameters]
    end

    private

    def post_init(plugin)
      @parameter_keys = ["sql_last_value"] + plugin.parameters.keys
      @parameters = plugin.parameters.inject({}) do |hash,(k,v)|
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

  class PreparedStatementHandler < StatementHandler
    attr_reader :name, :bind_values_array, :statement_prepared, :prepared

    def build_query(db, sql_last_value)
      bound_values = create_bind_values_hash
      if statement_prepared.false?
        prepended = bound_values.keys.map{|v| v.to_s.prepend("$").to_sym}
        @prepared = db[statement, *prepended].prepare(:select, name)
        statement_prepared.make_true
      end
      # under the scheduler the database is recreated each time
      # so the previous prepared statements are lost, add back
      if db.prepared_statement(name).nil?
        db.set_prepared_statement(name, prepared)
      end
      bind_value_sql_last_value(bound_values, sql_last_value)
      db.call(name, bound_values)
    end

    private

    def post_init(plugin)
      @name = plugin.prepared_statement_name.to_sym
      @bind_values_array = plugin.prepared_statement_bind_values
      @statement_prepared = Concurrent::AtomicBoolean.new(false)
    end

    def create_bind_values_hash
      hash = {}
      bind_values_array.each_with_index {|v,i| hash[:"p#{i}"] = v}
      hash
    end

    def bind_value_sql_last_value(hash, sql_last_value)
      hash.keys.each do |key|
        value = hash[key]
        if value == ":sql_last_value"
          hash[key] = sql_last_value
        end
      end
    end
  end
end end end
