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

module GroongaDelta
  class LTSVLogFormatter
    def call(severity, time, program_name, message)
      prefix = "timestamp:%{timestamp}\tseverity:%{severity}\tpid:%{pid}" % {
        severity: severity,
        timestamp: time.strftime("%Y-%m-%dT%H:%M:%S.%N"),
        pid: Process.pid,
      }
      formatted = ""
      backtrace = nil
      case message
      when String
      when Exception
        backtrace = message.backtrace
        message = "#{message.class}: #{message}"
      else
        message = message.inspect
      end
      message.each_line(chomp: true) do |line, i|
        formatted << "#{prefix}\tmessage:#{escape_value(line)}\n"
      end
      if backtrace
        backtrace.each do |trace|
          formatted << "#{prefix}\tmessage:#{escape_value(trace)}\n"
        end
      end
      formatted
    end

    private
    def escape_value(value)
      value.gsub(/[\t\r\n]/, " ")
    end
  end
end
