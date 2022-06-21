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

class ImportCommandLocalVacuumerTest < Test::Unit::TestCase
  include Helper

  def run_command(*args)
    command_line = GroongaDelta::ImportCommand.new
    command_line.run(["--dir=#{@dir}", *args])
  end

  def setup
    Dir.mktmpdir do |dir|
      @dir = dir
      @keep_seconds = 1
      data = {
        "vacuum" => {
          "keep_seconds" => @keep_seconds,
        },
      }
      File.open(File.join(@dir, "config.yaml"), "w") do |output|
        output.puts(data.to_yaml)
      end
      @config = GroongaDelta::ImportConfig.new(@dir)
      @writer = GroongaDelta::LocalWriter.new(@config)
      @reader = GroongaDelta::LocalReader.new(@config.logger, @config.delta_dir)
      yield
    end
  end

  def test_keep_seconds
    @writer.write_upserts("logs", {"_key": "log1"})
    paths = Dir.glob("#{@dir}/delta/**/*.grn")
    assert_true(run_command)
    assert_equal(paths, @reader.each.collect(&:path))
    sleep(@keep_seconds)
    assert_true(run_command)
    assert_equal([], @reader.each.collect(&:path))
  end
end
