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

require "groonga/command/parser"

require_relative "writer"

module GroongaDelta
  class LocalSource
    def initialize(config, status)
      @logger = config.logger
      @writer = Writer.new(@logger, config.delta_dir)
      @config = config.local
      @status = status.local
    end

    def import
      latest_number = @status.number || -1
      targets = []
      Dir.glob("#{@config.dir}/*.grn") do |path|
        case File.basename(path)
        when /\A\d+/
          number = Regexp.last_match[0]
          number = Integer(number, 10)
          next if number <= latest_number
          targets << [number, path]
        else
          next
        end
      end
      targets.sort_by! {|number, _path| number}
      parser = create_command_parser
      targets.each do |number, path|
        if latest_number == -1 and number > @config.initial_max_number
          @logger.info("Stopped initial import")
          break
        end
        @logger.info("Start importing: #{path}")
        File.open(path) do |input|
          last_line = nil
          input.each_line do |line|
            last_line = line
            parser << line
          end
          if last_line and not last_line.end_with?("\n")
            parser << line
          end
        end
        @logger.info("Imported: #{path}")
        @status.update("number" => number)
      end
    end

    private
    def create_command_parser
      parser = Groonga::Command::Parser.new

      parser.on_command do |command|
        write_command(command)
      end

      parser.on_load_columns do |command, columns|
        command[:columns] ||= columns.join(",")
      end

      split_load_chunk_size = 10000
      load_values = []
      parser.on_load_value do |command, value|
        unless command[:values]
          load_values << value
          if load_values.size == split_load_chunk_size
            write_load_command(command, load_values)
            load_values.clear
          end
        end
        command.original_source.clear
      end

      parser.on_load_complete do |command|
        if command[:values]
          write_load_command(command)
        else
          unless load_values.empty?
            write_load_command(command, load_values)
            load_values.clear
          end
        end
      end

      parser
    end

    def write_command(command)
      case command.command_name
      when "delete"
        if command[:key]
          @writer.write_deletes(command[:table], [command[:key]])
        else
          raise NotImplementedError,
                "delete by not _key isn't supported yet: #{command.to_s}"
        end
      else
        @writer.write_schema(command)
      end
    end

    def write_load_command(command, values=nil)
      columns = command.columns
      values ||= command.values
      if columns
        original_values = values
        values = Enumerator.new do |yielder|
          yielder << columns
          values.each do |value|
            yielder << value
          end
        end
      end
      @writer.write_upserts(command.table, values)
    end
  end
end
