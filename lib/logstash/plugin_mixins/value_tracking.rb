# encoding: utf-8
require "yaml" # persistence

module LogStash module PluginMixins
  class ValueTracking

    def self.build_last_value_tracker(plugin)
      if plugin.use_column_value && plugin.tracking_column_type == "numeric"
        # use this irrespective of the jdbc_default_timezone setting
        klass = NumericValueTracker
      else
        if plugin.jdbc_default_timezone.nil? || plugin.jdbc_default_timezone.empty?
          # no TZ stuff for Sequel, use Time
          klass = TimeValueTracker
        else
          # Sequel does timezone handling on DateTime only
          klass = DateTimeValueTracker
        end
      end

      handler = NullFileHandler.new(plugin.last_run_metadata_path)
      if plugin.record_last_run
        handler = FileHandler.new(plugin.last_run_metadata_path)
      end
      if plugin.clean_run
        handler.clean
      end

      instance = klass.new(handler)
    end

    attr_reader :value

    def initialize(handler)
      @file_handler = handler
      set_value(get_initial)
    end

    def get_initial
      # override in subclass
    end

    def set_value(value)
      # override in subclass
    end

    def write
      @file_handler.write(@value)
    end
  end


  class NumericValueTracker < ValueTracking
    def get_initial
      @file_handler.read || 0
    end

    def set_value(value)
      return unless value.is_a?(Numeric)
      @value = value
    end
  end

  class DateTimeValueTracker < ValueTracking
    def get_initial
      @file_handler.read || DateTime.new(1970)
    end

    def set_value(value)
      if value.respond_to?(:to_datetime)
        @value = value.to_datetime
      end
    end
  end

  class TimeValueTracker < ValueTracking
    def get_initial
      @file_handler.read || Time.at(0).utc
    end

    def set_value(value)
      if value.respond_to?(:to_time)
        @value = value.to_time
      end
    end
  end

  class FileHandler
    def initialize(path)
      @path = path
      @exists = ::File.exist?(@path)
    end

    def clean
      return unless @exists
      ::File.delete(@path)
      @exists = false
    end

    def read
      return unless @exists
      YAML.load(::File.read(@path))
    end

    def write(value)
      ::File.write(@path, YAML.dump(value))
      @exists = true
    end
  end

  class NullFileHandler
    def initialize(path)
    end

    def clean
    end

    def read
    end

    def write(value)
    end
  end
end end
