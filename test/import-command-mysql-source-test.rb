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

class ImportCommandMySQLSourceTest < Test::Unit::TestCase
  include Helper

  def run_command(*args)
    command_line = GroongaDelta::ImportCommand.new
    command_line.run(["--dir=#{@dir}", *args])
  end

  def setup
    Dir.mktmpdir do |dir|
      @dir = dir
      yield
    end
  end

  def generate_config(mysql_version, port, checksum)
    data = {
      "mysql" => {
        "host" => "127.0.0.1",
        "port" => port,
        "user" => "replicator",
        "password" => "replicator-password",
        "replication_client" => {
          "user" => "c-replicator",
          "password" => "client-replicator-password",
        },
        "select" => {
          "user" => "selector",
          "password" => "selector-password",
        },
        "checksum" => checksum,
      },
      "mapping" => {
        "items" => {
          "sources" => [
            {
              "database" => "importer",
              "table" => "shoes",
              "columns" => {
                "_key" => "shoes-%{id}",
                "id" => "%{id}",
                "name" => "%{name}",
                "name_text" => {
                  "expression" => "html_untag(name)",
                },
                "created_at" => {
                  "expression" => "created_at",
                  "source_column_names" => [
                    "created_at",
                  ],
                  "type" => "Time",
                },
                "source" => "shoes",
              },
            },
          ],
        },
      }
    }
    yield(data) if block_given?
    File.open(File.join(@dir, "config.yaml"), "w") do |output|
      output.puts(data.to_yaml)
    end
  end

  def run_mysqld(version)
    config = load_docker_compose_yml
    case version
    when "5.5"
      target_service = "mysql-#{version}-replica"
      source_service = "mysql-#{version}-source"
      up_service = "mysql-#{version}-nested-replica"
      checksum = nil
    else
      target_service = "mysql-#{version}-source"
      source_service = target_service
      up_service = target_service
      checksum = "crc32"
    end
    target_port = extract_service_port(config["services"][target_service])
    source_port = extract_service_port(config["services"][source_service])
    up_port = extract_service_port(config["services"][up_service])
    system(*docker_compose_command_line("down"))
    pid = spawn(*docker_compose_command_line("up", up_service))
    begin
      loop do
        begin
          Mysql2::Client.new(host: "127.0.0.1",
                             port: up_port,
                             username: "root")
        rescue Mysql2::Error::ConnectionError
          sleep(0.1)
        else
          break
        end
      end
      yield(target_port, source_port, checksum)
    ensure
      system(*docker_compose_command_line("down"))
      Process.waitpid(pid)
    end
  end

  def time_zone_offset
    Time.now.strftime("%:z")
  end

  def setup_initial_records(source_port)
    client = Mysql2::Client.new(host: "127.0.0.1",
                                port: source_port,
                                username: "root")
    client.query("SET GLOBAL time_zone = '#{time_zone_offset}'")
    client.query("SET time_zone = '#{time_zone_offset}'")
    client.query("CREATE DATABASE importer")
    client.query("USE importer")
    client.query(<<-SQL)
      CREATE TABLE shoes (
        id int PRIMARY KEY,
        name text,
        created_at timestamp DEFAULT CURRENT_TIMESTAMP
      );
    SQL
    client.query("INSERT INTO shoes VALUES " +
                 "(1, 'shoes <br> a', '2022-01-19 00:00:01'), " +
                 "(2, 'shoes <br> b', '2022-01-19 00:00:02'), " +
                 "(3, 'shoes <br> c', '2022-01-19 00:00:03')");
    client.close
  end

  def setup_changes(source_port)
    client = Mysql2::Client.new(host: "127.0.0.1",
                                port: source_port,
                                username: "root",
                                database: "importer")
    client.query("INSERT INTO shoes VALUES " +
                 "(10, 'shoes <br> A', '2022-01-19 00:00:10'), " +
                 "(20, 'shoes <br> B', '2022-01-19 00:00:20'), " +
                 "(30, 'shoes <br> C', '2022-01-19 00:00:30')");
    client.query("DELETE FROM shoes WHERE id >= 20")
    client.query("INSERT INTO shoes VALUES " +
                 "(40, 'shoes <br> D', '2022-01-19 00:00:40')");
    client.query("UPDATE shoes SET name = 'shoes <br> X' WHERE id = 40");
    client.close
  end

  def read_table_files(table)
    data = ""
    Dir.glob("#{@dir}/delta/data/#{table}/{*/,}*.{grn,parquet}").sort.each do |file|
      case File.extname(file)
      when ".grn"
        data << File.read(file)
      when ".parquet"
        data << Arrow::Table.load(file).to_s
      end
    end
    data
  end

  def read_log
    File.read(File.join(@dir, "log", "groonga-delta-import.log"))
  end

  data(:version, ["5.5", "5.7"])
  def test_basic
    run_mysqld(data[:version]) do |target_port, source_port, checksum|
      generate_config(data[:version], target_port, checksum)
      setup_initial_records(source_port)
      assert_true(run_command)
      assert_equal(<<-UPSERT, read_table_files("items"), read_log)
\t_key\tid\tname\tname_text\t               created_at\tsource
0\tshoes-1\t1 \tshoes <br> a\tshoes  a \t2022-01-19T00:00:01#{time_zone_offset}\tshoes 
1\tshoes-2\t2 \tshoes <br> b\tshoes  b \t2022-01-19T00:00:02#{time_zone_offset}\tshoes 
2\tshoes-3\t3 \tshoes <br> c\tshoes  c \t2022-01-19T00:00:03#{time_zone_offset}\tshoes 
      UPSERT
      setup_changes(source_port)
      assert_true(run_command)
      assert_equal(<<-DELTA, read_table_files("items"), read_log)
\t_key\tid\tname\tname_text\t               created_at\tsource
0\tshoes-1\t1 \tshoes <br> a\tshoes  a \t2022-01-19T00:00:01#{time_zone_offset}\tshoes 
1\tshoes-2\t2 \tshoes <br> b\tshoes  b \t2022-01-19T00:00:02#{time_zone_offset}\tshoes 
2\tshoes-3\t3 \tshoes <br> c\tshoes  c \t2022-01-19T00:00:03#{time_zone_offset}\tshoes 
load --table items
[
{"_key":"shoes-10","id":"10","name":"shoes <br> A","name_text":"shoes  A","created_at":"2022-01-19 00:00:10","source":"shoes"},
{"_key":"shoes-20","id":"20","name":"shoes <br> B","name_text":"shoes  B","created_at":"2022-01-19 00:00:20","source":"shoes"},
{"_key":"shoes-30","id":"30","name":"shoes <br> C","name_text":"shoes  C","created_at":"2022-01-19 00:00:30","source":"shoes"}
]
delete --key "shoes-20" --table "items"
delete --key "shoes-30" --table "items"
load --table items
[
{"_key":"shoes-40","id":"40","name":"shoes <br> D","name_text":"shoes  D","created_at":"2022-01-19 00:00:40","source":"shoes"}
]
load --table items
[
{"_key":"shoes-40","id":"40","name":"shoes <br> X","name_text":"shoes  X","created_at":"2022-01-19 00:00:40","source":"shoes"}
]
      DELTA
    end
  end
end
