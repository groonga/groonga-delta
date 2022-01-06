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
require "yaml"

module GroongaDelta
  class Status
    def initialize(dir)
      @dir = dir
      @path = File.join(@dir, "status.yaml")
      if File.exist?(@path)
        @data = YAML.load(File.read(@path))
      else
        @data = {}
      end
    end

    def [](key)
      @data[key]
    end

    def update(data)
      @data.update(data)
      FileUtils.mkdir_p(@dir)
      File.open(@path, "w") do |output|
        output.puts(YAML.dump(@data))
      end
    end
  end
end
