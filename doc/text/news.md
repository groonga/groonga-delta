# NEWS

## 1.0.2 - 2022-06-21

### Improvements

  * `import`: Added support for logging all MySQL replication event
    details by `debug` log level.

  * `import`: Improved error handling on record generation.

  * `import`: Added support for deleting a record by number/time key.

  * `import`: Added support for vacuuming old delta files.

### Fixes

  * `import`: Fixed a bug that retrying from an error may cause "no
    table map" error for row events. We need to retry from the last
    table map event.

  * `apply`: Fixed a bug that delta files not applied yet may not be
    applied.

## 1.0.1 - 2022-06-09

### Improvements

  * Added support for `SIGINT` by `SIGTERM`.

## 1.0.0 - 2022-03-07

Initial release.
