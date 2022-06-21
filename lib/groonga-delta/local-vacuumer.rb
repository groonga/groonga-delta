# Copyright (C) 2022  Sutou Kouhei <kou@clear-code.com>
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

require_relative "local-reader"

module GroongaDelta
  class LocalVacuumer
    def initialize(config)
      @logger = config.logger
      @delta_dir = config.delta_dir
      @config = config.vacuum
    end

    def vacuum
      keep_seconds = @config.keep_seconds
      return if keep_seconds.nil?
      return if keep_seconds < 0
      reader = LocalReader.new(@logger, @delta_dir)
      max_timestamp = Time.now.utc - keep_seconds
      reader.each(nil, max_timestamp) do |target|
        target.vacuum(@logger)
      end
    end
  end
end
