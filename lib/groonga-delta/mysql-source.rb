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

require "mysql2"

require_relative "writer"

module GroongaDelta
  class MySQLSource
    def initialize(config, status)
      @logger = config.logger
      @writer = Writer.new(config.delta_dir)
      @config = config.mysql
      @binlog_dir = @config.binlog_dir
      @mapping = config.mapping
      @status = status.mysql
      @tables = {}
    end

    def import
      case ENV["GROONGA_DELTA_IMPORT_MYSQL_SOURCE_BACKEND"]
      when "mysqlbinlog"
        require "mysql_binlog"
        import_mysqlbinlog
      when "mysql2-replication"
        require "mysql2-replication"
        import_mysql2_replication
      else
        begin
          require "mysql2-replication"
        rescue LoadError
          require "mysql_binlog"
          import_mysqlbinlog
        else
          import_mysql2_replication
        end
      end
    end

    private
    def import_mysqlbinlog
      file, position = read_current_status
      FileUtils.mkdir_p(@binlog_dir)
      local_file = File.join(@binlog_dir, file)
      unless File.exist?(local_file.succ)
        command_line = ["mysqlbinlog"]
        command_line << "--host=#{@config.host}" if @config.host
        command_line << "--port=#{@config.port}" if @config.port
        command_line << "--socket=#{@config.socket}" if @config.socket
        if @config.replication_slave_user
          command_line << "--user=#{@config.replication_slave_user}"
        end
        if @config.replication_slave_password
          command_line << "--password=#{@config.replication_slave_password}"
        end
        command_line << "--read-from-remote-server"
        command_line << "--raw"
        command_line << "--result-file=#{@binlog_dir}/"
        command_line << file
        spawn_process(*command_line) do |pid, output_read, error_read|
          wait_process(pid)
        end
      end
      reader = MysqlBinlog::BinlogFileReader.new(local_file)
      binlog = MysqlBinlog::Binlog.new(reader)
      binlog.checksum = @config.checksum
      binlog.ignore_rotate = true
      binlog.each_event do |event|
        next if event[:position] < position
        case event[:type]
        when :rotate_event
          @status.update("file" => event[:event][:name],
                         "position" => event[:event][:pos])
        when :write_rows_event_v1,
             :write_rows_event_v2,
             :update_rows_event_v1,
             :update_rows_event_v2,
             :delete_rows_event_v1,
             :delete_rows_event_v2
          normalized_type = event[:type].to_s.gsub(/_v\d\z/, "").to_sym
          import_rows_event(normalized_type,
                            event[:event][:table][:db],
                            event[:event][:table][:table],
                            file,
                            event[:header][:next_position]) do
            case normalized_type
            when :write_rows_event,
                 :update_rows_event
              event[:event][:row_image].collect do |row_image|
                build_row(row_image[:after][:image])
              end
            when :delete_rows_event
              event[:event][:row_image].collect do |row_image|
                build_row(row_image[:before][:image])
              end
            end
          end
          position = event[:header][:next_position]
        end
      end
    end

    def import_mysql2_replication
      file, position = read_current_status
      is_mysql_56_or_later = mysql(@config.select_user,
                                   @config.select_password) do |select_client|
        mysql_version(select_client) >= Gem::Version.new("5.6")
      end
      mysql(@config.replication_slave_user,
            @config.replication_slave_password) do |client|
        if is_mysql_56_or_later
          replication_client = Mysql2Replication::Client.new(client)
        else
          replication_client = Mysql2Replication::Client.new(client,
                                                             checksum: "NONE")
        end
        replication_client.file_name = file
        replication_client.start_position = position
        replication_client.open do
          replication_client.each do |event|
            case event
            when Mysql2Replication::RotateEvent
              file = event.file_name unless event.next_position.zero?
            when Mysql2Replication::RowsEvent
              event_name = event.class.name.split("::").last
              normalized_type =
              event_name.scan(/[A-Z][a-z]+/).
                collect(&:downcase).
                join("_").
                to_sym
              import_rows_event(normalized_type,
                                event.table_map.database,
                                event.table_map.table,
                                file,
                                event.next_position) do
                case normalized_type
                when :update_rows_event
                  event.updated_rows
                else
                  event.rows
                end
              end
            end
          end
        end
      end
    end

    def import_rows_event(type,
                          database_name,
                          table_name,
                          file,
                          next_position,
                          &block)
      source_table = @mapping[database_name, table_name]
      return if source_table.nil?

      table = find_table(database_name, table_name)
      groonga_table = source_table.groonga_table
      target_rows = block.call
      groonga_records = target_rows.collect do |row|
        record = build_record(table, row)
        groonga_table.generate_record(record)
      end
      return if groonga_records.empty?

      case type
      when :write_rows_event,
           :update_rows_event
        @writer.write_upserts(groonga_table.name, groonga_records)
      when :delete_rows_event
        groonga_record_keys = groonga_records.collect do |record|
          record[:_key]
        end
        @writer.write_deletes(groonga_table.name,
                              groonga_record_keys)
      end
      @status.update("file" => file,
                     "position" => next_position)
    end

    def wait_process(pid)
      begin
        _, status = Process.waitpid2(pid)
      rescue SystemCallError
      else
        unless status.success?
          message = "Failed to run: #{command_line.join(' ')}\n"
          message << "--- output ---\n"
          message << output_read.read
          message << "--------------\n"
          message << "--- error ----\n"
          message << error_read.read
          message << "--------------\n"
          raise message
        end
      end
    end

    def spawn_process(*command_line)
      env = {
        "LC_ALL" => "C",
      }
      output_read, output_write = IO.pipe
      error_read, error_write = IO.pipe
      options = {
        :out => output_write,
        :err => error_write,
      }
      pid = spawn(env, *command_line, options)
      output_write.close
      error_write.close
      if block_given?
        begin
          yield(pid, output_read, error_read)
        rescue
          begin
            Process.kill(:TERM, pid)
          rescue SystemCallError
          end
          raise
        ensure
          wait_process(pid)
          output_read.close unless output_read.closed?
          error_read.close unless error_read.closed?
        end
      else
        [pid, output_read, error_read]
      end
    end

    def mysql(user, password)
      options = {}
      options[:host] = @config.host if @config.host
      options[:port] = @config.port if @config.port
      options[:socket] = @config.socket if @config.socket
      options[:username] = user if user
      options[:password] = password if password
      client = Mysql2::Client.new(**options)
      begin
        yield(client)
      ensure
        client.close unless client.closed?
      end
    end

    def mysql_version(client)
      version = client.query("SELECT version()", as: :array).first.first
      Gem::Version.new(version)
    end

    def read_current_status
      if @status.file
        [@status.file, @status.position]
      else
        file = nil
        position = 0
        mysql(@config.replication_client_user,
              @config.replication_client_password) do |replication_client|
          replication_client.query("FLUSH TABLES WITH READ LOCK")
          result = replication_client.query("SHOW MASTER STATUS").first
          file = result["File"]
          position = result["Position"]
          mysql(@config.select_user,
                @config.select_password) do |select_client|
            start_transaction = "START TRANSACTION " +
                                "WITH CONSISTENT SNAPSHOT"
            if mysql_version(select_client) >= Gem::Version.new("5.6")
              start_transaction += ", READ ONLY"
            end
            select_client.query(start_transaction)
            replication_client.close
            import_existing_data(select_client)
            select_client.query("ROLLBACK")
          end
        end
        @status.update("file" => file,
                       "position" => position)
        [file, position]
      end
    end

    def import_existing_data(client)
      @mapping.source_databases.each do |source_database|
        source_database.source_tables.each do |source_table|
          statement = client.prepare(<<~SQL)
            SELECT COUNT(*) AS n_tables
            FROM information_schema.tables
            WHERE
              table_schema = ? AND
              table_name = ?
          SQL
          result = statement.execute(source_database.name,
                                     source_table.name)
          n_tables = result.first["n_tables"]
          statement.close
          next if n_tables.zero?
          full_table_name = "#{source_database.name}.#{source_table.name}"
          source_column_names = source_table.source_column_names
          column_list = source_column_names.join(", ")
          select = "SELECT #{column_list} FROM #{full_table_name}"
          if source_table.source_filter
            select << " WHERE #{source_table.source_filter}"
          end
          result = client.query(select,
                                symbolize_keys: true,
                                cache_rows: false,
                                stream: true)
          groonga_table_name = source_table.groonga_table.name
          @logger.info("Importing #{groonga_table_name} data " +
                       "from #{full_table_name}")
          groonga_records = Enumerator.new do |yielder|
            result.each do |row|
              groonga_record = source_table.groonga_table.generate_record(row)
              yielder << groonga_record
            end
          end
          @writer.write_upserts(source_table.groonga_table.name,
                                groonga_records,
                                packed: true)
          @logger.info("Imported #{groonga_table_name} data " +
                       "from #{full_table_name}")
        end
      end
    end

    def find_table(database_name, table_name)
      return @tables[table_name] if @tables.key?(table_name)

      mysql(@config.select_user,
            @config.select_password) do |client|
        statement = client.prepare(<<~SQL)
          SELECT column_name,
                 ordinal_position,
                 data_type,
                 column_key
          FROM information_schema.columns
          WHERE
            table_schema = ? AND
            table_name = ?
        SQL
        result = statement.execute(database_name, table_name)
        columns = result.collect do |column|
          {
            name: column["column_name"],
            ordinal_position: column["ordinal_position"],
            data_type: column["data_type"],
            is_primary_key: column["column_key"] == "PRI",
          }
        end
        @tables[table_name] = columns.sort_by do |column|
          column[:ordinal_position]
        end
      end
    end

    def build_row(value_pairs)
      row = {}
      value_pairs.each do |value_pair|
        value_pair.each do |column_index, value|
          row[column_index] = value
        end
      end
      row
    end

    def build_record(table, row)
      record = {}
      row.each do |column_index, value|
        record[table[column_index][:name].to_sym] = value
      end
      record
    end
  end
end
