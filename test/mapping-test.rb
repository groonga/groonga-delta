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

class MappingTest < Test::Unit::TestCase
  include Helper

  sub_test_case("Time") do
    def setup
      data = {
        "items" => {
          "restriction" => {
            "time" => {
              "max" => "2100-01-01T00:00:00Z",
              "min" => "1970-01-01T00:00:00Z",
            },
          },
          "sources" => [
            {
              "database" => "source",
              "table" => "shoes",
              "columns" => {
                "release_time" => {
                  "expression" => "release_time",
                  "type" => "Time",
                }
              },
            },
          ]
        },
      }
      @mapping = GroongaDelta::Mapping.new(data)
    end

    def generate_record(source_record)
      @mapping["source", "shoes"].groonga_table.generate_record(source_record)
    end

    def test_too_large
      assert_equal({release_time: Time.parse("2100-01-01T00:00:00Z").localtime},
                   generate_record({"release_time" => Time.new(2300)}))
    end

    def test_too_small
      assert_equal({release_time: Time.parse("1970-01-01T00:00:00Z").localtime},
                   generate_record({"release_time" => Time.new(1900)}))
    end
  end
end
