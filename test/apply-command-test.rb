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
  def run_command(*args)
    command_line = GroongaDelta::ApplyCommand.new
    Dir.chdir(@dir) do
      command_line.run(["--dir=#{@dir}", *args])
    end
  end

  def setup
    @host = "127.0.01"
    Dir.mktmpdir do |dir|
      @dir = dir
      @db_dir = "#{@dir}/db"
      @delta_dir = "#{@dir}/delta"
      @delta_schema_dir = "#{@delta_dir}/schema"
      @delta_data_dir = "#{@delta_dir}/data"
      yield
    end
  end

  def generate_config(port)
    data = {
      "delta_dir" => @delta_dir,
      "groonga" => {
        "url" => "http://#{@host}:#{port}",
      },
    }
    File.open(File.join(@dir, "config.yaml"), "w") do |output|
      output.puts(data.to_yaml)
    end
  end

  def decide_groonga_server_port
    static_port = 50041
    10.times do
      begin
        TCPServer.open(@host, static_port) do |server|
          return static_port
        end
      rescue SystemCallError
        sleep(0.1)
      end
    end

    dynamic_port = TCPServer.open(@host, 0) do |server|
      server.addr[1]
    end
    dynamic_port
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

  def open_groonga_client(port, &block)
    Groonga::Client.open(url: "http://#{@host}:#{port}",
                         backend: :synchronous,
                         &block)
  end

  def run_groonga
    FileUtils.mkdir_p(@db_dir)
    port = decide_groonga_server_port
    pid_file_path = Pathname("#{@db_dir}/groonga.pid")
    begin
      pid = spawn("groonga",
                  "--bind-address", @host,
                  "--log-path", "#{@db_dir}/groonga.log",
                  "--pid-path", pid_file_path.to_s,
                  "--port", port.to_s,
                  "--protocol", "http",
                  "--query-log-path", "#{@db_dir}/query.log",
                  "-s",
                  "-n",
                  "#{@db_dir}/db")
      begin
        begin
          n_retried = 0
          begin
            open_groonga_client(port) do |client|
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
            raise
          end
          retry
        end
        yield(port)
      ensure
        if pid
          open_groonga_client(port) do |client|
            client.shutdown
          end
          wait_groonga_http_shutdown(pid_file_path)
          Process.waitpid(pid)
        end
      end
    end
  end

  def dump_db
    output = Tempfile.new("groonga-sync-dump")
    pid = spawn("groonga", "#{@db_dir}/db", "dump",
                out: output.path)
    Process.waitpid(pid)
    output.read
  end

  def add_schema(schema, packed: false)
    timestamp = Time.now.utc
    timestamp_string = timestamp.strftime("%Y-%m-%d-%H-%M-%S-%N")
    path = "#{timestamp_string}.grn"
    if packed
      path = "packed/#{timestamp_string}/#{path}"
    end
    path = "#{@delta_schema_dir}/#{path}"
    FileUtils.mkdir_p(File.dirname(path))
    File.open(path, "w") do |input|
      input.puts(schema)
    end
  end

  def add_upsert_data(table, load, packed: false)
    timestamp = Time.now.utc
    timestamp_string = timestamp.strftime("%Y-%m-%d-%H-%M-%S-%N")
    path = "#{timestamp_string}-upsert.grn"
    if packed
      path = "packed/#{timestamp_string}/#{path}"
    end
    path = "#{@delta_data_dir}/#{table}/#{path}"
    FileUtils.mkdir_p(File.dirname(path))
    File.open(path, "w") do |input|
      input.puts(load)
    end
  end

  def test_packed_on_initialization
    run_groonga do |port|
      generate_config(port)
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
      assert_equal(<<-DUMP, dump_db)
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
    run_groonga do |port|
      generate_config(port)
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
      assert_equal(<<-DUMP, dump_db)
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
      assert_equal(<<-DUMP, dump_db)
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
    run_groonga do |port|
      generate_config(port)
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
      assert_equal(<<-DUMP, dump_db)
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
      assert_equal(<<-DUMP, dump_db)
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
    run_groonga do |port|
      generate_config(port)
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
      assert_equal(<<-DUMP, dump_db)
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
