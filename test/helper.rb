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

module Helper
  private
  def fixture_path(*components)
    File.join(__dir__, "fixture", *components)
  end

  def docker_compose_yml
    fixture_path("docker-compose.yml")
  end

  def load_docker_compose_yml
    YAML.load(File.read(docker_compose_yml))
  end

  def extract_service_port(service)
    Integer(service["ports"].first.split(":")[1], 10)
  end

  def docker_compose_command_line(*args)
    ["docker-compose", "--file", docker_compose_yml, *args]
  end
end
