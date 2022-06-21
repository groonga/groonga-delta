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

require "pp"

module GroongaDelta
  class Error < StandardError
  end

  class ConfigError < Error
  end

  class ExecutionError < Error
  end

  class ProcessError < Error
  end

  class GenerationError < Error
    attr_reader :source_record
    attr_reader :groonga_column
    attr_reader :detail
    def initialize(source_record, groonga_column, detail)
      @source_record = source_record
      @groonga_column = groonga_column
      @detail = detail
      message =
        "failed to generate a Groonga record:\n" +
        "source record: #{PP.pp(source_record, '')}" +
        "Groonga column: #{PP.pp(groonga_column, '')}" +
        "detail: #{@detail.message}(#{@detail.class})\n" +
        @detail.backtrace.join("\n")
      super(message)
    end
  end
end
