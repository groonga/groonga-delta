# Copyright (C) 2021-2022  Sutou Kouhei <kou@clear-code.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

module GroongaDelta
  class Mapping
    def initialize(data)
      @data = data
      build_source_databases
    end

    def source_databases
      @source_databases
    end

    def [](source_database_name, source_table_name=nil)
      if source_table_name.nil?
        @source_databases_index[source_database_name]
      else
        @source_tables_index[[source_database_name, source_table_name]]
      end
    end

    private
    def build_source_databases
      raw_source_databases = {}
      @data.each do |groonga_table_name, details|
        restriction = Restriction.new(details["restriction"])
        (details["sources"] || []).each do |source|
          raw_groonga_columns = source["columns"]
          groonga_columns = []
          source_column_names = []
          raw_groonga_columns.each do |name, template|
            if template.is_a?(Hash)
              options = template
              template = options["template"]
              expression = options["expression"]
              type = options["type"]
              source_column_names.concat(options["source_column_names"] || [])
            else
              expression = nil
              type = nil
            end
            groonga_columns << GroongaColumn.new(name,
                                                 template,
                                                 expression,
                                                 type,
                                                 restriction)
            if template
              template.scan(/%{(.*?)}/).flatten.each do |source_column_name|
                source_column_names << source_column_name.to_sym
              end
            end
          end
          source_column_names.uniq!
          groonga_table = GroongaTable.new(groonga_table_name,
                                           groonga_columns)
          source_table = SourceTable.new(source["table"],
                                         source_column_names,
                                         source["filter"],
                                         groonga_table)
          source_tables = (raw_source_databases[source["database"]] ||= [])
          source_tables << source_table
        end
      end
      @source_databases = []
      @source_databases_index = {}
      @source_tables_index = {}
      raw_source_databases.each do |source_database_name, source_tables|
        source_database = SourceDatabase.new(source_database_name,
                                             source_tables)
        @source_databases << source_database
        @source_databases_index[source_database.name] = source_database
        source_tables.each do |source_table|
          @source_tables_index[[source_database.name, source_table.name]] =
            source_table
        end
      end
    end

    class Restriction
      attr_reader :time_max
      attr_reader :time_min
      def initialize(data)
        @data = data
        @time_max = time_value("time", "max")
        @time_min = time_value("time", "min")
      end

      private
      def time_value(*keys)
        return nil if @data.nil?
        value = @data.dig(*keys)
        return value if value.nil?
        Time.parse(value).localtime
      end
    end

    class SourceDatabase
      attr_reader :name
      attr_reader :source_tables
      def initialize(name, source_tables)
        @name = name
        @source_tables = source_tables
      end
    end

    class SourceTable
      attr_reader :name
      attr_reader :source_column_names
      attr_reader :source_filter
      attr_reader :groonga_table
      def initialize(name, source_column_names, source_filter, groonga_table)
        @name = name
        @source_column_names = source_column_names
        @source_filter = source_filter
        @groonga_table = groonga_table
      end
    end

    class GroongaTable
      attr_reader :name
      attr_reader :groonga_columns
      def initialize(name, groonga_columns)
        @name = name
        @groonga_columns = groonga_columns
      end

      def generate_record(source_record)
        record = {}
        @groonga_columns.each do |groonga_column|
          value = groonga_column.generate_value(source_record)
          record[groonga_column.name.to_sym] = value
        end
        record
      end

      def generate_record_batch(source_records)
        fields = @groonga_columns.collect do |groonga_column|
          {
            name: groonga_column.name,
            data_type: groonga_column.arrow_type,
          }
        end
        builder = Arrow::RecordBatchBuilder.new(fields)
        groonga_records = Enumerator.new do |yielder|
          source_records.each do |source_record|
            yielder << generate_record(source_record)
          end
        end
        builder.append_records(groonga_records)
        builder.flush
      end
    end

    class GroongaColumn
      attr_reader :name
      attr_reader :template
      attr_reader :expression
      attr_reader :type
      attr_reader :restriction
      def initialize(name, template, expression, type, restriction)
        @name = name
        @template = template
        @expression = expression
        @type = type
        @restriction = restriction
      end

      def generate_value(source_record)
        if @template
          value = cast(@template % source_record)
        else
          evaluator = ExpressionEvaluator.new(source_record)
          value = evaluator.evaluate(@expression)
        end
        normalize_value(value)
      end

      def arrow_type
        case @type
        when nil, "ShortText", "Text", "LongText"
          :string
        when "Bool"
          :boolean
        when "Time"
          Arrow::TimestampDataType.new(:nano)
        else
          @type
        end
      end

      private
      def cast(value)
        case @type
        when nil, "ShortText", "Text", "LongText"
          value
        when /\AU?Int(?:8|16|32|64)\z/
          return 0 if value.empty?
          Integer(value, 10)
        when "Float"
          return 0.0 if value.empty?
          Float(value)
        when "Bool"
          return false if value.empty?
          case value
          when "0"
            false
          else
            true
          end
        when "Time"
          case value
          when /\A(\d{4})-(\d{2})-(\d{2})\z/
            match = Regexp.last_match
            year = Integer(match[1], 10)
            month = Integer(match[2], 10)
            day = Integer(match[3], 10)
            Time.new(year, month, day)
          when /\A(\d{4})-(\d{2})-(\d{2})\ 
                  (\d{2}):(\d{2}):(\d{2})\ 
                  ([+-])(\d{2})(\d{2})\z/x
            match = Regexp.last_match
            year = Integer(match[1], 10)
            month = Integer(match[2], 10)
            day = Integer(match[3], 10)
            hour = Integer(match[4], 10)
            minute = Integer(match[5], 10)
            second = Integer(match[6], 10)
            timezone_sign = match[7]
            timezone_hour = match[8]
            timezone_minute = match[9]
            timezone = "#{timezone_sign}#{timezone_hour}:#{timezone_minute}"
            time = Time.new(year, month, day, hour, minute, second, timezone)
            time.utc.localtime
          else
            value
          end
        else
          raise "Unknown type: #{@type}: #{value.inspect}"
        end
      end

      def normalize_value(value)
        case type
        when "Time"
          time_max = @restriction.time_max
          time_min = @restriction.time_min
          if !value.is_a?(Time) and value.respond_to?(:to_time)
            value = value.to_time
          end
          return value if time_max.nil? and time_min.nil?
          return value unless value.is_a?(Time)
          if time_max and value >= time_max
            time_max
          elsif time_min and value <= time_min
            time_min
          else
            value
          end
        else
          value
        end
      end
    end

    class ExpressionEvaluator
      class Context < BasicObject
        def html_untag(text)
          text.gsub(/<.*?>/, "")
        end

        def groonga_escape_query(text)
          case text
          when /[+\-><~*()"\\: ]/
            "\"#{text.gsub("\"", "\\\"")}\""
          when "OR"
            "\"OR\""
          else
            text
          end
        end
      end

      def initialize(source_record)
        @context = Context.new
        context_singleton_class =
          Kernel.instance_method(:singleton_class).bind(@context).call
        source_record.each do |key, value|
          context_singleton_class.define_method(key) do
            value
          end
        end
      end

      def evaluate(expression)
        @context.instance_eval(expression, __FILE__, __LINE__)
      end
    end
  end
end
