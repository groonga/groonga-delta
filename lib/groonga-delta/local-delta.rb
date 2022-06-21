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

require "groonga/client"

require_relative "local-reader"

module GroongaDelta
  class LocalDelta
    def initialize(config, status)
      @config = config
      @status = status
      @logger = @config.logger
      @delta_dir = @config.local.delta_dir
    end

    def apply
      reader = LocalReader.new(@logger, @delta_dir)
      start_time = read_current_status(reader)
      current_time = Time.now.utc
      client_options = {
        url: @config.groonga.url,
        read_timeout: @config.groonga.read_timeout,
        backend: :synchronous,
      }
      Groonga::Client.open(client_options) do |client|
        processor = CommandProcessor.new(@config,
                                         client,
                                         target_commands: [],
                                         target_tables: [],
                                         target_columns: [])
        reader.each(start_time, current_time) do |target|
          target.apply(@logger, client, processor)
          @status.update("start_time" => [
                           target.timestamp.to_i,
                           target.timestamp.nsec,
                         ])
        end
      end
    end

    private
    def read_current_status(reader)
      start_time_unix_time, start_time_nanosecond = @status.start_time
      if start_time_unix_time
        start_time = Time.at(start_time_unix_time).utc
        reader.build_time(start_time.year,
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

    class CommandProcessor < Groonga::Client::CommandProcessor
      def initialize(config, *args)
        @config = config
        super(*args)
      end

      def process_response(response, command)
        message = ""
        case command.command_name
        when "load"
          command.arguments.delete(:values)
          if response.success?
            message = "#{response.n_loaded_records}: "
          else
            load_response = Groonga::Client::Response::Load.new(command,
                                                                response.header,
                                                                response.body)
            message = "#{load_response.n_loaded_records}: "
          end
        end
        if response.success?
          @config.logger.info("Processed: " +
                              "#{response.elapsed_time}: " +
                              "#{command.command_name}: " +
                              message +
                              "#{command.to_command_format}")
        else
          failed_message = "Failed to process: " +
                           "#{response.return_code}: " +
                           "#{response.elapsed_time}: " +
                           "#{response.error_message}: " +
                           "#{command.command_name}: " +
                           message +
                           "#{command.to_command_format}"
          case @config.on_error
          when "ignore"
          when "warning"
            @config.logger.warn(failed_message)
          when "error"
            raise ExecutionError, failed_message
          end
        end
      end
    end
  end
end
