# encoding: utf-8
require "zk"

module LogStash module PluginMixins module Jdbc
  class ValueTrackingZookeeper

    def self.build_last_value_tracker(plugin)
      if plugin.use_column_value && plugin.tracking_column_type == "numeric"
        # use this irrespective of the jdbc_default_timezone setting
        klass = NumericValueTrackerZK
      else
        if plugin.jdbc_default_timezone.nil? || plugin.jdbc_default_timezone.empty?
          # no TZ stuff for Sequel, use Time
          klass = TimeValueTrackerZK
        else
          # Sequel does timezone handling on DateTime only
          klass = DateTimeValueTrackerZK
        end
      end

      handler = NullNodeHandler.new(plugin.last_run_zookeeper_path)
      if plugin.record_last_run
        handler = NodeHandler.new(plugin)
      end
      if plugin.clean_run
        handler.clean
      end
      instance = klass.new(handler)
      return instance
    end

    attr_reader :value

    def initialize(handler)
      @node_handler = handler
      set_value(read_value)
    end

    def read_value
      # override in subclass
    end

    def set_value(value)
      # override in subclass
    end

    def write
      @node_handler.write(@value)
    end
  end


  class NumericValueTrackerZK < ValueTrackingZookeeper
    def read_value
      @val = @node_handler.read
      return 0 if @val.nil?
      @val.to_f.round(0)
    end

    def set_value(value)
      return unless value.is_a?(Numeric)
      @value = value
    end
  end

  class DateTimeValueTrackerZK < ValueTrackingZookeeper
    def read_value
      @node_handler.read || DateTime.new(1970)
    end

    def set_value(value)
      if value.respond_to?(:to_datetime)
        @value = value.to_datetime
      else
        @value = DateTime.parse(value)
      end
    end
  end

  class TimeValueTrackerZK < ValueTrackingZookeeper
    def read_value
      @node_handler.read || Time.at(0).utc
    end

    def set_value(value)
      if value.respond_to?(:to_time)
        @value = value.to_time
      else
        @value = DateTime.parse(value).to_time
      end
    end
  end

  class NodeHandler
    def initialize(plugin)
      @path = plugin.last_run_zookeeper_path
      @zk_ip_list = plugin.zk_ip_list
      @zk_ephemeral = plugin.zk_ephemeral

      @zk = ZK.new(@zk_ip_list)
      @exists = @zk.exists?(@path)
      create_node
    end

    def clean
      return unless @exists
      @zk.delete(@path)
      @exists = false
    end

    def read
      return unless @exists
      @zk.get(@path).first
    end

    def set_initial(initial)
      @initial = initial
    end

    def create_node
      unless @exists
        if @zk_ephemeral
          @zk.create(@path, :ephemeral => true)
        else
          @zk.create(@path)
        end
        @exists = true
      end
    end

    def write(value)
      @zk.set(@path, value.to_s)
    end
  end

  class NullNodeHandler
    def initialize(path)
    end

    def clean
    end

    def read
    end

    def write(value)
    end
  end
end end end
