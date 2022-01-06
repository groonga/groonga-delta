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
require_relative "apply-config"
require_relative "apply-status"

require_relative "local-delta"

module GroongaDelta
  class ApplyCommand < Command
    private
    def prepare
      @config = ApplyConfig.new(@dir)
      @status = ApplyStatus.new(@dir)
      @deltas = []
      @deltas << LocalDelta.new(@config, @status)
    end

    def process
      @deltas.each do |delta|
        delta.apply
      end
    end
  end
end
