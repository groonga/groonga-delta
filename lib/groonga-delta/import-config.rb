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

require_relative "config"
require_relative "mapping"

module GroongaDelta
  class ImportConfig < Config
    def initialize(dir)
      super("groonga-delta-import", dir)
    end

    def delta_dir
      resolve_path(@data["delta_dir"] || "delta")
    end

    def mysql
      return nil unless @data["mysql"]
      MySQL.new(@dir,
                @data["mysql"],
                @secret_data["mysql"] || {})
    end

    def local
      return nil unless @data["local"]
      Local.new(@dir, @data["local"])
    end

    def mapping
      Mapping.new(@data["mapping"] || {})
    end

    class MySQL
      include Config::PathResolvable

      def initialize(dir, data, secret_data)
        @dir = dir
        @data = data
        @secret_data = secret_data
      end

      def binlog_dir
        resolve_path(@data["binlog_dir"] || "binlog")
      end

      def mysqlbinlog
        @data["mysqlbinlog"] || "mysqlbinlog"
      end

      def host
        @data["host"] || "localhost"
      end

      def port
        @data["port"] || 3306
      end

      def socket
        @data["socket"]
      end

      def user
        @data["user"]
      end

      def password
        @secret_data["password"] || @data["password"]
      end

      def replication_client
        @data["replication_client"] || @data
      end

      def replication_client_user
        replication_client["user"]
      end

      def replication_client_password
        (@secret_data["replication_client"] || @secret_data)["password"] ||
          replication_client["password"]
      end

      def replication_slave
        @data["replication_slave"] || @data
      end

      def replication_slave_user
        replication_slave["user"]
      end

      def replication_slave_password
        (@secret_data["replication_slave"] || @secret_data)["password"] ||
          replication_slave["password"]
      end

      def select
        @data["select"] || @data
      end

      def select_user
        select["user"]
      end

      def select_password
        (@secret_data["select"] || @secret_data)["password"] ||
          select["password"]
      end

      def checksum
        _checksum = @data["checksum"]
        return nil if _checksum.nil?
        _checksum.to_sym
      end

      def initial_import_batch_size
        resolve_size(@data["initial_import_batch_size"] || 1024 * 1024)
      end

      private
      def resolve_size(value)
        case value
        when String
          case value
          when /\A(\d+)[kK]\z/
            Integer($1, 10) * 1024
          when /\A(\d+)[mM]\z/
            Integer($1, 10) * 1024 * 1024
          when /\A(\d+)[gG]\z/
            Integer($1, 10) * 1024 * 1024 * 1024
          else
            raise ArgumentError, "invalid size value: #{value.inspect}"
          end
        else
          value
        end
      end
    end

    class Local
      include Config::PathResolvable

      def initialize(dir, data)
        @dir = dir
        @data = data
      end

      def dir
        resolve_path(@data["dir"] || "local")
      end

      def initial_max_number
        @data["initial_max_number"] || Float::INFINITY
      end
    end
  end
end
