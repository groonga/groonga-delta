#!/usr/bin/env ruby
#
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

# $VERBOSE = true

test_dir = __dir__

ENV["TEST_UNIT_MAX_DIFF_TARGET_STRING_SIZE"] ||= "10000"

require "socket"
require "stringio"
require "tempfile"
require "tmpdir"

require "parquet"
require "mysql2"

require "test-unit"

require_relative "../lib/groonga-delta"

require_relative "helper"

exit(Test::Unit::AutoRunner.run(true, test_dir))
