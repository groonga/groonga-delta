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
    end

    def groonga
      Groonga.new(@dir, @data["groonga"] || {})
    end

    def local
      Local.new(@dir, @data["local"] || {})
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
        if @data.key?("read_timeout")
          ::Groonga::Client::Default::READ_TIMEOUT
        else
          @data["read_timeout"]
        end
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
