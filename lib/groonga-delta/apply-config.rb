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

require_relative "config"

module GroongaDelta
  class ApplyConfig < Config
    def initialize(dir)
      super("groonga-delta-apply", dir)
      validate_on_error(on_error)
    end

    def on_error
      @data["on_error"] || "error"
    end

    def groonga
      Groonga.new(@dir, @data["groonga"] || {})
    end

    def local
      Local.new(@dir, @data["local"] || {})
    end

    private
    def validate_on_error(on_error)
      case on_error
      when "ignore"
      when "warning"
      when "error"
      else
        message = "on_error must be ignore, warning or error: " +
                  on_error.inspect
        raise ConfigError, message
      end
    end

    class Groonga
      def initialize(dir, data)
        @dir = dir
        @data = data
      end

      def url
        @data["url"] || "http://127.0.0.1:10041"
      end

      def read_timeout
        @data["read_timeout"] || ::Groonga::Client::Default::READ_TIMEOUT
      end
    end

    class Local
      include Config::PathResolvable

      def initialize(dir, data)
        @dir = dir
        @data = data
      end

      def delta_dir
        resolve_path(@data["delta_dir"] || "delta")
      end
    end
  end
end
