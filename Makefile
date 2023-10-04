sqlfs: sqlfs.c
	clang -g -l fuse3 -l sqlite3 -o sqlfs sqlfs.c