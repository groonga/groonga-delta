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

require_relative "status"

module GroongaDelta
  class ImportStatus < Status
    def mysql
      MySQL.new(self)
    end

    def local
      Local.new(self)
    end

    class MySQL
      def initialize(status)
        @status = status
      end

      def [](key)
        (@status["mysql"] || {})[key]
      end

      def update(new_data)
        @status.update("mysql" => new_data)
      end

      def last_file
        self["last_file"] || self["file"] # For backward compatibility
      end

      def last_position
        self["last_position"] || self["position"] # For backward compatibility
      end

      def last_table_map_file
        self["last_table_map_file"] || self["file"]
      end

      def last_table_map_position
        self["last_table_map_position"]
      end
    end

    class Local
      def initialize(status)
        @status = status
      end

      def [](key)
        (@status["local"] || {})[key]
      end

      def update(new_data)
        @status.update("local" => new_data)
      end

      def number
        self["number"]
      end
    end
  end
end
