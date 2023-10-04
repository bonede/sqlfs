# SQLfs - Linux FUSE file system backed by SQLite

## Dependency
* libc
* libfuse3
* libsqlite3

## Test drive
```console
$ # Build it
$ make
$ # Prepare the mount point
$ mkdir ~/fs
$ # Mount the file system in `~/fs`. Files are stored in sqlite file `~/fs.db`. `-f` make it run in foreground
$ ./sqlfs -f --db ~/fs.db ~/fs 
$ # In another terminal
$ git clone git@github.com:bonede/sqlfs.git ~/fs
```

