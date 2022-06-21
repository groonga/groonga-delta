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

require_relative "command"
require_relative "import-config"
require_relative "import-status"
require_relative "local-writer"
require_relative "local-vacuumer"

module GroongaDelta
  class ImportCommand < Command
    private
    def prepare
      @config = ImportConfig.new(@dir)
      @status = ImportStatus.new(@dir)
      @writer = LocalWriter.new(@config)
      @vacuumer = LocalVacuumer.new(@config)
      @sources = []
      if @config.local
        require_relative "local-source"
        @sources << LocalSource.new(@config, @status, @writer)
      end
      if @config.mysql
        require_relative "mysql-source"
        @sources << MySQLSource.new(@config, @status, @writer)
      end
    end

    def process
      @sources.each do |source|
        source.import
      end
      @vacuumer.vacuum
    end
  end
end
