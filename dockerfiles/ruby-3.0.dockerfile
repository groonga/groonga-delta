FROM ruby:3.0-bullseye

RUN \
  echo "debconf debconf/frontend select Noninteractive" | \
    debconf-set-selections

RUN \
  apt-get update && \
  apt-get install -y -V \
    ca-certificates \
    lsb-release \
    wget && \
  wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
  apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
  rm apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb && \
  apt-get update && \
  apt-get install -y -V \
    cmake \
    git \
    libgirepository1.0-dev \
    libparquet-glib-dev \
    ninja-build && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

RUN \
  git config --global user.name "groonga" && \
  git config --global user.email "groonga@groonga.org"

RUN \
  git clone \
    --branch 3.2 \
    https://github.com/mariadb-corporation/mariadb-connector-c.git && \
  git -C mariadb-connector-c fetch origin pull/187/head:fake-rotate-event && \
  git -C mariadb-connector-c checkout fake-rotate-event && \
  git -C mariadb-connector-c rebase 3.2 && \
  git -C mariadb-connector-c checkout - && \
  git -C mariadb-connector-c merge fake-rotate-event && \
  cmake \
    -S mariadb-connector-c \
    -B mariadb-connector-c.build \
    -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DINSTALL_LIBDIR=lib \
    -DWITH_EXTERNAL_ZLIB=ON \
    -DWITH_UNIT_TESTS=OFF && \
  ninja -C mariadb-connector-c.build install && \
  rm -rf mariadb-connector-c*

RUN \
  gem install \
    mysql2-replication \
    mysql_binlog \
    red-parquet

COPY . groonga-delta
WORKDIR groonga-delta
RUN rake install
WORKDIR /
RUN rm -rf groonga-delta
