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

class InportConfigTest < Test::Unit::TestCase
  include Helper

  sub_test_case("MySQL") do
    sub_test_case("#initial_import_batch_size") do
      def initial_import_batch_size(input)
        data = {
          "initial_import_batch_size" => input,
        }
        config = GroongaDelta::ImportConfig::MySQL.new(".", data, {})
        config.initial_import_batch_size
      end

      def test_k
        assert_equal(512 * 1024,
                     initial_import_batch_size("512k"))
      end

      def test_K
        assert_equal(512 * 1024,
                     initial_import_batch_size("512K"))
      end

      def test_m
        assert_equal(512 * 1024 * 1024,
                     initial_import_batch_size("512m"))
      end

      def test_M
        assert_equal(512 * 1024 * 1024,
                     initial_import_batch_size("512M"))
      end

      def test_g
        assert_equal(512 * 1024 * 1024 * 1024,
                     initial_import_batch_size("512g"))
      end

      def test_G
        assert_equal(512 * 1024 * 1024 * 1024,
                     initial_import_batch_size("512G"))
      end

      def test_unknown_unit
        message = "invalid size value: \"512X\""
        assert_raise(GroongaDelta::ConfigError.new(message)) do
          initial_import_batch_size("512X")
        end
      end
    end
  end

  sub_test_case("Vacuum") do
    sub_test_case("#keep_span") do
      def keep_span(input)
        data = {
          "keep_span" => input,
        }
        config = GroongaDelta::ImportConfig::Vacuum.new(data)
        config.keep_span
      end

      data("integer", [1, 1])
      data("float", [1.1, 1.1])
      data("string - no unit", [1.0, "1"])
      ["s", "sec", "second", "seconds"].each do |unit|
        data("string - #{unit} - integer", [1.0, "1#{unit}"])
        data("string - #{unit} - float", [1.1, "1.1#{unit}"])
      end
      ["m", "min", "minute", "minutes"].each do |unit|
        data("string - #{unit} - integer", [1.0 * 60, "1#{unit}"])
        data("string - #{unit} - float", [1.1 * 60, "1.1#{unit}"])
      end
      ["h", "hr", "hour", "hours"].each do |unit|
        data("string - #{unit} - integer", [1.0 * 60 * 60, "1#{unit}"])
        data("string - #{unit} - float", [1.1 * 60 * 60, "1.1#{unit}"])
      end
      ["d", "day", "days"].each do |unit|
        data("string - #{unit} - integer", [1.0 * 60 * 60 * 24, "1#{unit}"])
        data("string - #{unit} - float", [1.1 * 60 * 60 * 24, "1.1#{unit}"])
      end
      ["w", "week", "weeks"].each do |unit|
        data("string - #{unit} - integer", [1.0 * 60 * 60 * 24 * 7, "1#{unit}"])
        data("string - #{unit} - float", [1.1 * 60 * 60 * 24 * 7, "1.1#{unit}"])
      end
      ["month", "months"].each do |unit|
        data("string - #{unit} - integer",
             [1.0 * 60 * 60 * 24 * 30.44, "1#{unit}"])
        data("string - #{unit} - float",
             [1.1 * 60 * 60 * 24 * 30.44, "1.1#{unit}"])
      end
      ["y", "year", "years"].each do |unit|
        data("string - #{unit} - integer",
             [1.0 * 60 * 60 * 24 * 365.25, "1#{unit}"])
        data("string - #{unit} - float",
             [1.1 * 60 * 60 * 24 * 365.25, "1.1#{unit}"])
      end
      def test_input(data)
        expected, actual = data
        assert_equal(expected, keep_span(actual))
      end

      def test_string_unknown_unit
        message = "invalid span value: \"1X\""
        assert_raise(GroongaDelta::ConfigError.new(message)) do
          keep_span("1X")
        end
      end
    end
  end
end
