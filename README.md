# Groonga delta

## Description

Groonga delta provides delta based data import tools. They can import data to Groonga from other systems such as local file system, MySQL and PostgreSQL.

## Usage

### Import deltas from other systems

Create configuration file: TODO

Run `groonga-delta-import` with the created configuration file:

```bash
docker run \
  --rm \
  --volume /var/lib/groonga-delta:/var/lib/groonga-delta:z \
  ghcr.io/groonga/groonga-delta:latest \
  groonga-delta-import \
    --server \
    --dir=/var/lib/groonga-delta/import
```

### Apply imported deltas

Create configuration file: TODO

Run `groonga-delta-apply` with the created configuration file:

```bash
docker run \
  --rm \
  --volume /var/lib/groonga-delta:/var/lib/groonga-delta:z \
  ghcr.io/groonga/groonga-delta:latest \
  groonga-delta-apply \
    --server \
    --dir=/var/lib/groonga-delta/apply
```

## License

GPLv3 or later. See `LICENSE.txt` for details.
