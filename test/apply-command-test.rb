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

class ApplyCommandTest < Test::Unit::TestCase
  include Helper

  def run_command(*args)
    command_line = GroongaDelta::ApplyCommand.new
    command_line.run(["--dir=#{@dir}", *args])
  end

  def setup
    @host = "127.0.0.1"
    @port = extract_service_port(load_docker_compose_yml["services"]["groonga"])
    Dir.mktmpdir do |dir|
      @dir = dir
      @delta_dir = "#{@dir}/delta"
      @delta_schema_dir = "#{@delta_dir}/schema"
      @delta_data_dir = "#{@delta_dir}/data"
      yield
    end
  end

  def generate_config
    data = {
      "delta_dir" => @delta_dir,
      "groonga" => {
        "url" => "http://#{@host}:#{@port}",
      },
    }
    File.open(File.join(@dir, "config.yaml"), "w") do |output|
      output.puts(data.to_yaml)
    end
  end

  def wait_groonga_http_shutdown(pid_file_path)
    total_sleep_time = 0
    sleep_time = 0.1
    shutdown_wait_timeout = 5
    while pid_file_path.exist?
      sleep(sleep_time)
      total_sleep_time += sleep_time
      break if total_sleep_time > shutdown_wait_timeout
    end
  end

  def open_groonga_client(&block)
    Groonga::Client.open(url: "http://#{@host}:#{@port}",
                         backend: :synchronous,
                         &block)
  end

  def run_groonga
    begin
      system(*docker_compose_command_line("down"))
      pid = spawn(*docker_compose_command_line("up", "groonga"))
      begin
        begin
          n_retried = 0
          begin
            open_groonga_client do |client|
              client.status
            end
          rescue Groonga::Client::Error
            n_retried += 1
            sleep(0.1)
            retry if n_retried < 100
            raise
          end
        rescue
          if Process.waitpid(pid, Process::WNOHANG)
            pid = nil
            system(*docker_compose_command_line("down"))
            raise
          end
          retry
        end
        yield
      ensure
        if pid
          open_groonga_client do |client|
            client.shutdown
          end
          system(*docker_compose_command_line("down"))
          Process.waitpid(pid)
        end
      end
    end
  end

  def dump_db
    open_groonga_client do |client|
      client.dump.raw
    end
  end

  def add_schema(schema,
                 per_day: false,
                 packed: false)
    timestamp = Time.now.utc
    timestamp_string = timestamp.strftime("%Y-%m-%d-%H-%M-%S-%N")
    path = "#{timestamp_string}.grn"
    if packed
      path = "packed/#{timestamp_string}/#{path}"
    elsif per_day
      day = timestamp.strftime("%Y-%m-%d")
      path = "#{day}/#{path}"
    end
    path = "#{@delta_schema_dir}/#{path}"
    FileUtils.mkdir_p(File.dirname(path))
    File.open(path, "w") do |input|
      input.puts(schema)
    end
  end

  def add_upsert_data(table,
                      load_or_arrow_table,
                      per_day: false,
                      packed: false)
    timestamp = Time.now.utc
    timestamp_string = timestamp.strftime("%Y-%m-%d-%H-%M-%S-%N")
    path = "#{timestamp_string}-upsert"
    if load_or_arrow_table.is_a?(Arrow::Table)
      path << ".parquet"
    else
      path << ".grn"
    end
    if packed
      path = "packed/#{timestamp_string}/#{path}"
    elsif per_day
      day = timestamp.strftime("%Y-%m-%d")
      path = "#{day}/#{path}"
    end
    path = "#{@delta_data_dir}/#{table}/#{path}"
    FileUtils.mkdir_p(File.dirname(path))
    if load_or_arrow_table.is_a?(Arrow::Table)
      load_or_arrow_table.save(path, format: :parquet)
    else
      File.open(path, "w") do |input|
        input.puts(load_or_arrow_table)
      end
    end
  end

  def test_packed_on_initialization
    run_groonga do
      generate_config
      add_schema(<<-SCHEMA, packed: true)
table_create Items TABLE_HASH_KEY ShortText
column_create Items name COLUMN_SCALAR ShortText
column_create Items price COLUMN_SCALAR UInt32
      SCHEMA
      add_upsert_data("Items", <<-LOAD, packed: true)
load --table Items
[
{"_key": "item1", "name": "Shoes"},
{"_key": "item2", "name": "Hat"}
]
      LOAD
      assert_true(run_command)
      assert_equal(<<-DUMP.chomp, dump_db)
table_create Items TABLE_HASH_KEY ShortText
column_create Items name COLUMN_SCALAR ShortText
column_create Items price COLUMN_SCALAR UInt32

load --table Items
[
["_key","name","price"],
["item1","Shoes",0],
["item2","Hat",0]
]
      DUMP
    end
  end

  def test_packed_after_initialization
    run_groonga do
      generate_config
      add_schema(<<-SCHEMA, packed: true)
table_create Items TABLE_HASH_KEY ShortText
column_create Items name COLUMN_SCALAR ShortText
column_create Items price COLUMN_SCALAR UInt32
      SCHEMA
      add_upsert_data("Items", <<-LOAD, packed: true)
load --table Items
[
{"_key": "item1", "name": "Shoes"},
{"_key": "item2", "name": "Hat"}
]
      LOAD
      assert_true(run_command)
      assert_equal(<<-DUMP.chomp, dump_db)
table_create Items TABLE_HASH_KEY ShortText
column_create Items name COLUMN_SCALAR ShortText
column_create Items price COLUMN_SCALAR UInt32

load --table Items
[
["_key","name","price"],
["item1","Shoes",0],
["item2","Hat",0]
]
      DUMP

      # Not used
      add_schema(<<-SCHEMA, packed: true)
column_remove Items price
      SCHEMA
      assert_true(run_command)
      assert_equal(<<-DUMP.chomp, dump_db)
table_create Items TABLE_HASH_KEY ShortText
column_create Items name COLUMN_SCALAR ShortText
column_create Items price COLUMN_SCALAR UInt32

load --table Items
[
["_key","name","price"],
["item1","Shoes",0],
["item2","Hat",0]
]
      DUMP
    end
  end

  def test_non_packed_after_initialization
    run_groonga do
      generate_config
      add_schema(<<-SCHEMA, packed: true)
table_create Items TABLE_HASH_KEY ShortText
column_create Items name COLUMN_SCALAR ShortText
column_create Items price COLUMN_SCALAR UInt32
      SCHEMA
      add_upsert_data("Items", <<-LOAD, packed: true)
load --table Items
[
{"_key": "item1", "name": "Shoes"},
{"_key": "item2", "name": "Hat"}
]
      LOAD
      assert_true(run_command)
      assert_equal(<<-DUMP.chomp, dump_db)
table_create Items TABLE_HASH_KEY ShortText
column_create Items name COLUMN_SCALAR ShortText
column_create Items price COLUMN_SCALAR UInt32

load --table Items
[
["_key","name","price"],
["item1","Shoes",0],
["item2","Hat",0]
]
      DUMP

      add_schema(<<-SCHEMA)
column_remove Items price
      SCHEMA
      assert_true(run_command)
      assert_equal(<<-DUMP.chomp, dump_db)
table_create Items TABLE_HASH_KEY ShortText
column_create Items name COLUMN_SCALAR ShortText

load --table Items
[
["_key","name"],
["item1","Shoes"],
["item2","Hat"]
]
      DUMP
    end
  end

  def test_non_packed_on_initialization
    run_groonga do
      generate_config
      add_schema(<<-SCHEMA)
table_create Items TABLE_HASH_KEY ShortText
column_create Items name COLUMN_SCALAR ShortText
column_create Items price COLUMN_SCALAR UInt32
      SCHEMA
      add_upsert_data("Items", <<-LOAD)
load --table Items
[
{"_key": "item1", "name": "Shoes"},
{"_key": "item2", "name": "Hat"}
]
      LOAD
      assert_true(run_command)
      assert_equal(<<-DUMP.chomp, dump_db)
table_create Items TABLE_HASH_KEY ShortText
column_create Items name COLUMN_SCALAR ShortText
column_create Items price COLUMN_SCALAR UInt32

load --table Items
[
["_key","name","price"],
["item1","Shoes",0],
["item2","Hat",0]
]
      DUMP
    end
  end

  def test_per_day_only
    run_groonga do
      generate_config
      assert_true(run_command)
      add_schema(<<-SCHEMA, per_day: true)
table_create Items TABLE_HASH_KEY ShortText
column_create Items name COLUMN_SCALAR ShortText
column_create Items price COLUMN_SCALAR UInt32
      SCHEMA
      add_upsert_data("Items", <<-LOAD, per_day: true)
load --table Items
[
{"_key": "item1", "name": "Shoes"},
{"_key": "item2", "name": "Hat"}
]
      LOAD
      assert_true(run_command)
      assert_equal(<<-DUMP.chomp, dump_db)
table_create Items TABLE_HASH_KEY ShortText
column_create Items name COLUMN_SCALAR ShortText
column_create Items price COLUMN_SCALAR UInt32

load --table Items
[
["_key","name","price"],
["item1","Shoes",0],
["item2","Hat",0]
]
      DUMP
    end
  end

  def test_no_per_day_only
    run_groonga do
      generate_config
      assert_true(run_command)
      add_schema(<<-SCHEMA)
table_create Items TABLE_HASH_KEY ShortText
column_create Items name COLUMN_SCALAR ShortText
column_create Items price COLUMN_SCALAR UInt32
      SCHEMA
      add_upsert_data("Items", <<-LOAD)
load --table Items
[
{"_key": "item1", "name": "Shoes"},
{"_key": "item2", "name": "Hat"}
]
      LOAD
      assert_true(run_command)
      assert_equal(<<-DUMP.chomp, dump_db)
table_create Items TABLE_HASH_KEY ShortText
column_create Items name COLUMN_SCALAR ShortText
column_create Items price COLUMN_SCALAR UInt32

load --table Items
[
["_key","name","price"],
["item1","Shoes",0],
["item2","Hat",0]
]
      DUMP
    end
  end

  def test_per_day_mixed
    run_groonga do
      generate_config
      assert_true(run_command)
      add_schema(<<-SCHEMA, per_day: true)
table_create Items TABLE_HASH_KEY ShortText
      SCHEMA
      add_schema(<<-SCHEMA)
column_create Items name COLUMN_SCALAR ShortText
      SCHEMA
      add_schema(<<-SCHEMA, per_day: true)
column_create Items price COLUMN_SCALAR UInt32
      SCHEMA
      add_upsert_data("Items", <<-LOAD, per_day: true)
load --table Items
[
{"_key": "item1", "name": "Shoes"}
]
      LOAD
      add_upsert_data("Items", <<-LOAD)
load --table Items
[
{"_key": "item2", "name": "Hat"}
]
      LOAD
      add_upsert_data("Items", <<-LOAD, per_day: true)
load --table Items
[
{"_key": "item3", "name": "T-shirt"}
]
      LOAD
      assert_true(run_command)
      assert_equal(<<-DUMP.chomp, dump_db)
table_create Items TABLE_HASH_KEY ShortText
column_create Items name COLUMN_SCALAR ShortText
column_create Items price COLUMN_SCALAR UInt32

load --table Items
[
["_key","name","price"],
["item1","Shoes",0],
["item2","Hat",0],
["item3","T-shirt",0]
]
      DUMP
    end
  end

  def test_upsert_parquet
    run_groonga do
      generate_config
      add_schema(<<-SCHEMA)
table_create Items TABLE_HASH_KEY ShortText
column_create Items name COLUMN_SCALAR ShortText
column_create Items price COLUMN_SCALAR UInt32
      SCHEMA
      table = Arrow::Table.new("_key" => ["item1", "item2"],
                               "name" => ["Shoes", "Hat"])
      add_upsert_data("Items", table)
      assert_true(run_command)
      assert_equal(<<-DUMP.chomp, dump_db)
table_create Items TABLE_HASH_KEY ShortText
column_create Items name COLUMN_SCALAR ShortText
column_create Items price COLUMN_SCALAR UInt32

load --table Items
[
["_key","name","price"],
["item1","Shoes",0],
["item2","Hat",0]
]
      DUMP
    end
  end
end
