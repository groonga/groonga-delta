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

require "fileutils"
require "json"

require "groonga/command"
require "parquet"

module GroongaDelta
  class Writer
    def initialize(logger, dir)
      @logger = logger
      @dir = dir
    end

    def write_upserts(table, records, packed: false)
      if records.is_a?(Arrow::Table)
        write_data(table,
                   "upsert",
                   ".parquet",
                   packed: packed,
                   open_output: false) do |output|
          records.save(output, format: :parquet)
        end
      else
        write_data(table, "upsert", ".grn", packed: packed) do |output|
          first_record = true
          records.each do |record|
            if first_record
              output.puts("load --table #{table}")
              output.print("[")
              first_record = false
            else
              output.print(",")
            end
            output.puts
            output.print(record.to_json)
          end
          unless first_record
            output.puts()
            output.puts("]")
          end
        end
      end
    end

    def write_deletes(table, keys)
      write_data(table, "delete", ".grn") do |output|
        delete = Groonga::Command::Delete.new
        delete[:table] = table
        keys.each do |key|
          delete[:key] = key
          output.puts(delete.to_command_format)
        end
      end
    end

    def write_schema(command)
      write_entry("schema", ".grn") do |output|
        output.puts(command.to_command_format)
      end
    end

    private
    def write_entry(prefix, suffix, packed: false, open_output: true)
      timestamp = Time.now.utc
      base_name = timestamp.strftime("%Y-%m-%d-%H-%M-%S-%N#{suffix}")
      if packed
        dir = "#{@dir}/#{prefix}/packed"
        packed_dir_base_name = timestamp.strftime("%Y-%m-%d-%H-%M-%S-%N")
        temporary_path = "#{dir}/.#{packed_dir_base_name}/#{base_name}"
        path = "#{dir}/#{packed_dir_base_name}/#{base_name}"
      else
        dir = "#{@dir}/#{prefix}"
        temporary_path = "#{dir}/.#{base_name}"
        path = "#{dir}/#{base_name}"
      end
      @logger.info("Start writing: #{temporary_path}")
      FileUtils.mkdir_p(File.dirname(temporary_path))
      if open_output
        File.open(temporary_path, "w") do |output|
          yield(output)
        end
      else
        yield(temporary_path)
      end
      if packed
        FileUtils.mv(File.dirname(temporary_path),
                     File.dirname(path))
      else
        FileUtils.mv(temporary_path, path)
      end
      @logger.info("Wrote: #{path}")
    end

    def write_data(table,
                   action,
                   suffix,
                   packed: false,
                   open_output: true,
                   &block)
      write_entry("data/#{table}",
                  "-#{action}#{suffix}",
                  packed: packed,
                  open_output: open_output,
                  &block)
    end
  end
end
