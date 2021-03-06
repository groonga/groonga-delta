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
require "logger"
require "yaml"

require_relative "ltsv-log-formatter"

class Logger::LogDevice
  private
  def add_log_header(file)
    # Disable adding log header
  end
end

module GroongaDelta
  class Config
    module PathResolvable
      private
      def resolve_path(path)
        File.expand_path(path, @dir)
      end
    end

    include PathResolvable

    def initialize(name, dir)
      @name = name
      @dir = dir
      @path = File.join(@dir, "config.yaml")
      if File.exist?(@path)
        @data = YAML.load(File.read(@path))
      else
        @data = {}
      end
      @secret_path = File.join(@dir, "secret.yaml")
      if File.exist?(@secret_path)
        @secret_data = YAML.load(File.read(@secret_path))
      else
        @secret_data = {}
      end
    end

    def logger
      @logger ||= create_logger
    end

    def log_path
      resolve_path(File.join(@data["log_dir"] || "log",
                             "#{@name}.log"))
    end

    def log_age
      @data["log_age"] || 100
    end

    def log_max_size
      @data["log_max_size"] || (1024 * 1024)
    end

    def log_period_suffix
      @data["log_period_suffix"] || "%Y-%m-%d"
    end

    def log_level
      @data["log_level"] || "info"
    end

    def polling_interval
      Float(@data["polling_interval"] || "60")
    end

    private
    def create_logger
      path = log_path
      FileUtils.mkdir_p(File.dirname(path))
      Logger.new(path,
                 log_age,
                 log_max_size,
                 formatter: LTSVLogFormatter.new,
                 level: log_level,
                 progname: @name,
                 shift_period_suffix: log_period_suffix)
    end
  end
end
