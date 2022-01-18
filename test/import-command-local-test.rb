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

class ImportCommandLocalTest < Test::Unit::TestCase
  include Helper

  def run_command(*args)
    command_line = GroongaDelta::ImportCommand.new
    command_line.run(["--dir=#{@dir}", *args])
  end

  def setup
    Dir.mktmpdir do |dir|
      @dir = dir
      @local_dir = File.join(@dir, "local")
      FileUtils.mkdir_p(@local_dir)
      yield
    end
  end

  def generate_config(local)
    data = {
      "local" => local,
    }
    File.open(File.join(@dir, "config.yaml"), "w") do |output|
      output.puts(data.to_yaml)
    end
  end

  def setup_initial_data
    File.open(File.join(@local_dir, "001-schema.grn"), "w") do |output|
      output.puts(<<-GROONGA_COMMAND)
table_create items TABLE_HASH_KEY ShortText
column_create items name COLUMN_SCALAR ShortText
      GROONGA_COMMAND
    end
    File.open(File.join(@local_dir, "002-load.grn"), "w") do |output|
      output.puts(<<-GROONGA_COMMAND)
load --table items
[
{"_key": "shoes-1", "name": "shoes a"},
{"_key": "shoes-2", "name": "shoes b"},
{"_key": "shoes-3", "name": "shoes c"}
]
      GROONGA_COMMAND
    end
    File.open(File.join(@local_dir, "010-index.grn"), "w") do |output|
      output.puts(<<-GROONGA_COMMAND)
table_create terms TABLE_PAT_KEY ShortText \
  --default_tokenizer TokenNgram \
  --normalizer NormalizerNFKC130
column_create terms items_name COLUMN_INDEX|WITH_POSITION items name
      GROONGA_COMMAND
    end
  end

  def setup_changes
    File.open(File.join(@local_dir, "100-update.grn"), "w") do |output|
      output.puts(<<-GROONGA_COMMAND)
load --table items
[
{"_key": "shoes-10", "name": "shoes A"},
{"_key": "shoes-20", "name": "shoes B"},
{"_key": "shoes-30", "name": "shoes C"}
]

delete --table items --key shoes-20
delete --table items --key shoes-30

load --table items
[
{"_key": "shoes-40", "name": "shoes D"}
]
load --table items
[
{"_key": "shoes-10", "name": "shoes X"}
]
      GROONGA_COMMAND
    end
  end

  def read_delta_files
    data = ""
    files = Dir.glob("#{@dir}/delta/{schema,data/*}/*.{grn,parquet}")
    files = files.sort_by do |file|
      File.basename(file)
    end
    files.each do |file|
      case File.extname(file)
      when ".grn"
        data << File.read(file)
      when ".parquet"
        data << Arrow::Table.load(file).to_s
      end
    end
    data
  end

  def test_default
    generate_config({})
    setup_initial_data
    assert_true(run_command)
    assert_equal(<<-DELTA, read_delta_files)
table_create --flags "TABLE_HASH_KEY" --key_type "ShortText" --name "items"
column_create --flags "COLUMN_SCALAR" --name "name" --table "items" --type "ShortText"
load --table items
[
{"_key":"shoes-1","name":"shoes a"},
{"_key":"shoes-2","name":"shoes b"},
{"_key":"shoes-3","name":"shoes c"}
]
table_create --default_tokenizer "TokenNgram" --flags "TABLE_PAT_KEY" --key_type "ShortText" --name "terms" --normalizer "NormalizerNFKC130"
column_create --flags "COLUMN_INDEX|WITH_POSITION" --name "items_name" --source "name" --table "terms" --type "items"
    DELTA
    setup_changes
    assert_true(run_command)
    assert_equal(<<-DELTA, read_delta_files)
table_create --flags "TABLE_HASH_KEY" --key_type "ShortText" --name "items"
column_create --flags "COLUMN_SCALAR" --name "name" --table "items" --type "ShortText"
load --table items
[
{"_key":"shoes-1","name":"shoes a"},
{"_key":"shoes-2","name":"shoes b"},
{"_key":"shoes-3","name":"shoes c"}
]
table_create --default_tokenizer "TokenNgram" --flags "TABLE_PAT_KEY" --key_type "ShortText" --name "terms" --normalizer "NormalizerNFKC130"
column_create --flags "COLUMN_INDEX|WITH_POSITION" --name "items_name" --source "name" --table "terms" --type "items"
load --table items
[
{"_key":"shoes-10","name":"shoes A"},
{"_key":"shoes-20","name":"shoes B"},
{"_key":"shoes-30","name":"shoes C"}
]
delete --key "shoes-20" --table "items"
delete --key "shoes-30" --table "items"
load --table items
[
{"_key":"shoes-40","name":"shoes D"}
]
load --table items
[
{"_key":"shoes-10","name":"shoes X"}
]
    DELTA
  end

  def test_initial_max_number
    generate_config({"initial_max_number" => 9})
    setup_initial_data
    assert_true(run_command)
    assert_equal(<<-DELTA, read_delta_files)
table_create --flags "TABLE_HASH_KEY" --key_type "ShortText" --name "items"
column_create --flags "COLUMN_SCALAR" --name "name" --table "items" --type "ShortText"
load --table items
[
{"_key":"shoes-1","name":"shoes a"},
{"_key":"shoes-2","name":"shoes b"},
{"_key":"shoes-3","name":"shoes c"}
]
    DELTA
    setup_changes
    assert_true(run_command)
    assert_equal(<<-DELTA, read_delta_files)
table_create --flags "TABLE_HASH_KEY" --key_type "ShortText" --name "items"
column_create --flags "COLUMN_SCALAR" --name "name" --table "items" --type "ShortText"
load --table items
[
{"_key":"shoes-1","name":"shoes a"},
{"_key":"shoes-2","name":"shoes b"},
{"_key":"shoes-3","name":"shoes c"}
]
table_create --default_tokenizer "TokenNgram" --flags "TABLE_PAT_KEY" --key_type "ShortText" --name "terms" --normalizer "NormalizerNFKC130"
column_create --flags "COLUMN_INDEX|WITH_POSITION" --name "items_name" --source "name" --table "terms" --type "items"
load --table items
[
{"_key":"shoes-10","name":"shoes A"},
{"_key":"shoes-20","name":"shoes B"},
{"_key":"shoes-30","name":"shoes C"}
]
delete --key "shoes-20" --table "items"
delete --key "shoes-30" --table "items"
load --table items
[
{"_key":"shoes-40","name":"shoes D"}
]
load --table items
[
{"_key":"shoes-10","name":"shoes X"}
]
    DELTA
  end
end
