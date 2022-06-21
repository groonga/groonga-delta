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

require "groonga/command"
require "parquet"

require_relative "error"

module GroongaDelta
  class LocalReader
    def initialize(logger, dir)
      @logger = logger
      @dir = dir
    end

    def each(min_timestamp=nil, max_timestamp=nil, &block)
      unless block_given?
        return to_enum(__method__, min_timestamp, max_timestamp)
      end

      targets = list_targets(@dir, min_timestamp, max_timestamp)
      targets.sort_by(&:timestamp).each(&block)
    end

    def build_time(year, month, day, hour=0, minute=0, second=0, nanosecond=0)
      Time.utc(year,
               month,
               day,
               hour,
               minute,
               Rational(second * 1_000_000_000 + nanosecond,
                        1_000_000_000))
    end

    private
    def each_target_path(dir,
                         min_timestamp,
                         max_timestamp,
                         accept_directory: true,
                         &block)
      if min_timestamp
        min_timestamp_day = Time.utc(min_timestamp.year,
                                     min_timestamp.month,
                                     min_timestamp.day)
      end
      if max_timestamp
        max_timestamp_day = Time.utc(max_timestamp.year,
                                     max_timestamp.month,
                                     max_timestamp.day)
      end
      Dir.glob("#{dir}/*") do |path|
        base_name = File.basename(path)
        if accept_directory and File.directory?(path)
          timestamp = parse_directory_timestamp(base_name)
          next if timestamp.nil?
          next if min_timestamp_day and timestamp < min_timestamp_day
          next if max_timestamp_day and timestamp > max_timestamp_day
          each_target_path(path,
                           min_timestamp,
                           max_timestamp,
                           accept_directory: false,
                           &block)
        elsif File.file?(path)
          timestamp, action, post_match = parse_file_timestamp(base_name)
          next if timestamp.nil?
          next if min_timestamp and timestamp <= min_timestamp
          next if max_timestamp and timestamp > max_timestamp
          yield(path, timestamp, action, post_match)
        end
      end
    end

    def each_packed_target_path(dir, min_timestamp, max_timestamp)
      return unless min_timestamp.to_i.zero?
      Dir.glob("#{dir}/packed/*") do |path|
        next unless File.directory?(path)
        timestamp, action, post_match = parse_file_timestamp(File.basename(path))
        next if action
        next unless post_match.empty?
        yield(path, timestamp)
      end
    end

    def list_targets(dir, min_timestamp, max_timestamp)
      targets = []
      list_schema_targets(dir, min_timestamp, max_timestamp, targets)
      Dir.glob("#{dir}/data/*") do |path|
        next unless File.directory?(path)
        name = File.basename(path)
        list_table_targets(path, name, min_timestamp, max_timestamp, targets)
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

    def list_schema_targets(dir, min_timestamp, max_timestamp, targets)
      latest_packed_target = nil
      each_packed_target_path("#{dir}/schema",
                              min_timestamp,
                              max_timestamp) do |path, timestamp|
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
                         latest_packed_target&.timestamp || min_timestamp,
                         max_timestamp) do |target|
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

    def list_table_targets(dir, name, min_timestamp, max_timestamp, targets)
      latest_packed_target = nil
      each_packed_target_path(dir,
                              min_timestamp,
                              max_timestamp) do |path, timestamp|
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
                        latest_packed_target&.timestamp || min_timestamp,
                        max_timestamp) do |target|
        targets << target
      end
    end

    def parse_directory_timestamp(base_name)
      case base_name
      when /\A(\d{4})-(\d{2})-(\d{2})\z/
        match = Regexp.last_match
        year = match[1].to_i
        month = match[2].to_i
        day = match[3].to_i
        build_time(year, month, day)
      else
        nil
      end
    end

    def parse_file_timestamp(base_name)
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

    module Loggable
      private
      def log(logger, path, before_message, after_message)
        logger.info("#{before_message}: #{path}")
        result = yield
        logger.info("#{after_message}: #{path}")
        result
      end

      def apply_log(logger, path, &block)
        log(logger, path, "Start applying", "Applied", &block)
      end

      def vacuum_log(logger, path, &block)
        log(logger, path, "Start vacuuming", "Vacuumed", &block)
      end
    end

    class SchemaTarget
      include Loggable

      attr_reader :path
      attr_reader :timestamp
      def initialize(path, timestamp)
        @path = path
        @timestamp = timestamp
      end

      def apply(logger, client, processor)
        apply_log(logger, @path) do
          processor.load(@path)
        end
      end

      def vacuum(logger)
        vacuum_log(logger, @path) do
          FileUtils.rm(@path)
        end
      end
    end

    class PackedSchemaTarget
      include Loggable

      attr_reader :path
      attr_reader :timestamp
      attr_reader :targets
      def initialize(path, timestamp)
        @path = path
        @timestamp = timestamp
        @targets = []
      end

      def apply(logger, client, processor)
        apply_log(logger, @path) do
          @targets.sort_by(&:timestamp).each do |target|
            target.apply(logger, client, processor)
          end
        end
      end

      def vacuum(logger)
        vacuum_log(logger, @path) do
          @targets.sort_by(&:timestamp).each do |target|
            target.vacuum(logger)
          end
        end
      end
    end

    class TableTarget
      include Loggable

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

      def apply(logger, client, processor)
        apply_log(logger, @path) do
          if @path.end_with?(".grn")
            processor.load(@path)
          else
            # TODO: Add support for @action == "delete"
            table = Arrow::Table.load(@path)
            command = Groonga::Command::Load.new(table: @name,
                                                 values: table,
                                                 command_version: "3")
            response = client.load(command.arguments)
            processor.process_response(response, command)
          end
        end
      end

      def vacuum(logger)
        vacuum_log(logger, @path) do
          FileUtils.rm(@path)
        end
      end
    end

    class PackedTableTarget
      include Loggable

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

      def apply(logger, client, processor)
        apply_log(logger, @path) do
          @targets.sort_by(&:timestamp).each do |target|
            target.apply(logger, client, processor)
          end
        end
      end

      def vacuum(logger)
        vacuum_log(logger, @path) do
          @targets.sort_by(&:timestamp).each do |target|
            target.vacuum(logger)
          end
        end
      end
    end
  end
end
