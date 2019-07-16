# encoding: UTF-8
# frozen_string_literal: true

require 'tzinfo'

if defined?(TZInfo::VERSION) && TZInfo::VERSION > '2'
  module TZInfo
    # A time represented as an `Integer` number of seconds since 1970-01-01
    # 00:00:00 UTC (ignoring leap seconds), the fraction through the second
    # (sub_second as a `Rational`) and an optional UTC offset. Like Ruby's `Time`
    # class, {Timestamp} can distinguish between a local time with a zero offset
    # and a time specified explicitly as UTC.
    class Timestamp

      protected

      def new_datetime(klass = DateTime)
        val = JD_EPOCH + ((@value.to_r + @sub_second) / 86400)
        datetime = klass.jd(jruby_scale_down_rational(val))
        @utc_offset && @utc_offset != 0 ? datetime.new_offset(Rational(@utc_offset, 86400)) : datetime
      end

      private

      # while this JRuby bug exists in 9.2.X.X https://github.com/jruby/jruby/issues/5791
      # we must scale down the numerator and denominator to fit Java Long values.

      def jruby_scale_down_rational(rat)
        return rat if rat.numerator <= java.lang.Long::MAX_VALUE
        [10, 100, 1000].each do |scale_by|
          new_numerator = rat.numerator / scale_by
          if new_numerator  <= java.lang.Long::MAX_VALUE
            return Rational(new_numerator, rat.denominator / scale_by)
          end
        end
      end
    end
  end
end
