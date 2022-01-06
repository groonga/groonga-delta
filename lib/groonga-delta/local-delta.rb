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

require "groonga/client"

module GroongaDelta
  class LocalDelta
    def initialize(config, status)
      @config = config
      @status = status
      @logger = @config.logger
      @delta_dir = @config.local.delta_dir
    end

    def apply
      start_time = read_current_status
      current_time = Time.now.utc
      targets = list_targets(@delta_dir, start_time, current_time)
      client_options = {
        url: @config.groonga.url,
        read_timeout: @config.groonga.read_timeout,
        backend: :synchronous,
      }
      Groonga::Client.open(client_options) do |client|
        processor = CommandProcessor.new(client,
                                         target_commands: [],
                                         target_tables: [],
                                         target_columns: [])
        targets.sort_by(&:timestamp).each do |target|
          target.sync(client, processor)
          @status.update("start_time" => [
                           target.timestamp.to_i,
                           target.timestamp.nsec,
                         ])
        end
      end
    end

    private
    def build_time(year, month, day, hour, minute, second, nanosecond)
      Time.utc(year,
               month,
               day,
               hour,
               minute,
               Rational(second * 1_000_000_000 + nanosecond,
                        1_000_000_000))
    end

    def read_current_status
      start_time_unix_time, start_time_nanosecond = @status.start_time
      if start_time_unix_time
        start_time = Time.at(start_time_unix_time).utc
        build_time(start_time.year,
                   start_time.month,
                   start_time.day,
                   start_time.hour,
                   start_time.min,
                   start_time.sec,
                   start_time_nanosecond)
      else
        Time.at(0).utc
      end
    end

    def each_target_path(dir, min_timestamp, max_timestamp)
      Dir.glob("#{dir}/*") do |path|
        next unless File.file?(path)
        timestamp, action, post_match = parse_timestamp(File.basename(path))
        next if timestamp.nil?
        next if min_timestamp and timestamp <= min_timestamp
        next if max_timestamp and timestamp > max_timestamp
        yield(path, timestamp, action, post_match)
      end
    end

    def each_packed_target_path(dir, min_timestamp, max_timestamp)
      return unless min_timestamp.to_i.zero?
      Dir.glob("#{dir}/packed/*") do |path|
        next unless File.directory?(path)
        timestamp, action, post_match = parse_timestamp(File.basename(path))
        next if action
        next unless post_match.empty?
        yield(path, timestamp)
      end
    end

    def list_targets(dir, start_time, current_timestamp)
      targets = []
      list_schema_targets(dir, start_time, current_timestamp, targets)
      Dir.glob("#{dir}/data/*") do |path|
        next unless File.directory?(path)
        name = File.basename(path)
        list_table_targets(path, name, start_time, current_timestamp, targets)
      end
      targets
    end

    def each_schema_target(dir, min_timestamp, max_timestamp)
      each_target_path(dir,
                       min_timestamp,
                       max_timestamp) do |path, timestamp, action, post_match|
        next if action
        next unless post_match == ".grn"
        yield(SchemaTarget.new(path, timestamp))
      end
    end

    def list_schema_targets(dir, start_time, current_timestamp, targets)
      latest_packed_target = nil
      each_packed_target_path("#{dir}/schema",
                              start_time,
                              current_timestamp) do |path, timestamp|
        if latest_packed_target and latest_packed_target.timestamp > timestamp
          next
        end
        latest_packed_target = PackedSchemaTarget.new(path, timestamp)
      end
      if latest_packed_target
        targets << latest_packed_target
        each_schema_target(latest_packed_target.path, nil, nil) do |target|
          latest_packed_target.targets << target
        end
      end
      each_schema_target("#{dir}/schema",
                         latest_packed_target&.timestamp || start_time,
                         current_timestamp) do |target|
        targets << target
      end
    end

    TABLE_TARGET_SUFFIXES = [".grn", ".parquet"]
    def each_table_target(dir, name, min_timestamp, max_timestamp)
      each_target_path(dir,
                       min_timestamp,
                       max_timestamp) do |path, timestamp, action, post_match|
        next if action.nil?
        next unless TABLE_TARGET_SUFFIXES.include?(post_match)
        yield(TableTarget.new(path, timestamp, name, action))
      end
    end

    def list_table_targets(dir, name, start_time, current_timestamp, targets)
      latest_packed_target = nil
      each_packed_target_path(dir,
                              start_time,
                              current_timestamp) do |path, timestamp|
        if latest_packed_target and latest_packed_target.timestamp > timestamp
          next
        end
        latest_packed_target = PackedTableTarget.new(path, timestamp, name)
      end
      if latest_packed_target
        targets << latest_packed_target
        each_table_target(latest_packed_target.path, name, nil, nil) do |target|
          latest_packed_target.targets << target
        end
      end
      each_table_target(dir,
                        name,
                        latest_packed_target&.timestamp || start_time,
                        current_timestamp) do |target|
        targets << target
      end
    end

    def parse_timestamp(base_name)
      case base_name
      when /\A(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{9})(?:-(\w+))?/
        match = Regexp.last_match
        year = match[1].to_i
        month = match[2].to_i
        day = match[3].to_i
        hour = match[4].to_i
        minute = match[5].to_i
        second = match[6].to_i
        nanosecond = match[7].to_i
        action = match[8]
        timestamp = build_time(year,
                               month,
                               day,
                               hour,
                               minute,
                               second,
                               nanosecond)
        [timestamp, action, match.post_match]
      else
        nil
      end
    end

    class SchemaTarget
      attr_reader :path
      attr_reader :timestamp
      def initialize(path, timestamp)
        @path = path
        @timestamp = timestamp
      end

      def sync(client, processor)
        processor.load(@path)
      end
    end

    class PackedSchemaTarget
      attr_reader :path
      attr_reader :timestamp
      attr_reader :targets
      def initialize(path, timestamp)
        @path = path
        @timestamp = timestamp
        @targets = []
      end

      def sync(client, processor)
        @targets.sort_by(&:timestamp).each do |target|
          target.sync(client, processor)
        end
      end
    end

    class TableTarget
      attr_reader :path
      attr_reader :timestamp
      attr_reader :name
      attr_reader :action
      def initialize(path, timestamp, name, action)
        @path = path
        @timestamp = timestamp
        @name = name
        @action = action
      end

      def sync(client, processor)
        if @path.end_with?(".grn")
          processor.load(@path)
        else
          # TODO: Add support for @action == "delete"
          table = Arrow::Table.load(@path)
          command = Groonga::Command::Load.new(table: @name,
                                               values: table)
          response = client.load(table: command[:table],
                                 values: command[:values])
          processor.process_response(response, command)
        end
      end
    end

    class PackedTableTarget
      attr_reader :path
      attr_reader :timestamp
      attr_reader :name
      attr_reader :targets
      def initialize(path, timestamp, name)
        @path = path
        @timestamp = timestamp
        @name = name
        @targets = []
      end

      def sync(client, processor)
        @targets.sort_by(&:timestamp).each do |target|
          target.sync(client, processor)
        end
      end
    end

    class CommandProcessor < Groonga::Client::CommandProcessor
      def process_response(response, command)
        # TODO
      end
    end
  end
end
