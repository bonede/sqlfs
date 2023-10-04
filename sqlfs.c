// benchmarking:
// https://docs.gitlab.com/ee/administration/operations/filesystem_benchmarking.html

#define _FILE_OFFSET_BITS 64
#define FUSE_USE_VERSION 35

#include <assert.h>
#include <errno.h>
#include <fuse3/fuse.h>
#include <fuse3/fuse_lowlevel.h>
#include <fuse3/fuse_opt.h>
#include <libgen.h>
#include <sqlite3.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define MAX_PATH_LEN 1024
#define OK 0
#define ROOT_DIR_MODE S_IFDIR | 0755
#define MIN(a, b) ((a) < (b) ? (a) : (b))

const char *create_tables_sql = "PRAGMA journal_mode=WAL;\n\
create table if not exists files(id integer primary key autoincrement, nlink integer default 1 not null, content blob, dev integer, size integer default 0);\n\
create table if not exists paths(id integer primary key autoincrement, path text not null, parent_id integer, uid integer not null, gid integer not null, mode integer not null, atime integer not null, mtime integer not null, ctime integer not null, file_id integer);\n\
create unique index if not exists path_idx on paths(path);\n\
create index if not exists file_id_idx on paths(file_id);\n\
";

const char *select_file_by_path_sql =
    "select f.* from paths p left join files f on p.file_id = f.id where "
    "p.path = ?";
const char *select_path_by_name_sql =
    "select p.uid, p.gid, p.mode, p.atime, p.mtime, p.ctime, ifnull(f.size, "
    "0) "
    "size, f.nlink nlink from paths p left join files f on p.file_id = f.id "
    "where p.path = ?";
const char *select_file_id_by_path_sql =
    "select file_id, mode mode from paths where path = ?";
const char *select_path_id_by_path_sql = "select id from paths where path = ?";
const char *select_stats_by_parent_id_sql =
    "select p.path, p.uid, p.gid, p.mode, p.atime, p.mtime, p.ctime, f.size "
    "size, f.nlink nlink from paths p left join files f on p.file_id = f.id "
    "where p.parent_id = ? limit -1 offset ?";
const char *insert_path_sql = "insert into paths(path, parent_id, uid, gid, "
                              "mode, atime, mtime, ctime, "
                              "file_id) values(?, ?, ?, ?, ?, ?, ?, ?, ?)";
const char *insert_file_sql =
    "insert into files(content, dev, size) values(?, ?, ?)";

const char *delete_path_by_id_sql = "delete from paths where id = ?";
const char *delete_file_by_id_sql = "delete from files where id = ?";
const char *increase_file_nlink_by_id_sql =
    "update files set nlink = nlink + 1 where id = ?";
const char *decrease_file_nlink_by_id_sql =
    "update files set nlink = nlink - 1 where id = ?";
const char *select_file_nlink_by_id_sql =
    "select nlink from files where id = ?";
const char *select_path_info_by_path_sql =
    "select p.id id, p.mode mode, p.file_id file_id, ifnull(f.size, 0) size "
    "from paths p left join files f on p.file_id = f.id where path = ?";
const char *count_dir_items_by_id_sql =
    "select count(id) from paths where id = ?";
const char *update_path_times_by_id_sql =
    "update paths set atime = ?, mtime = ? where id = ?";
const char *select_file_content_by_id_sql =
    "select content from files where id = ?";

const char *update_path_name_by_id_sql =
    "update paths set path = ? where id = ?";
const char *update_path_mode_by_id_sql =
    "update paths set mode = ? where id = ?";
const char *update_path_owner_by_id_sql =
    "update paths set uid = ?, gid = ? where id = ?";
const char *update_file_size_by_id_sql =
    "update files set size = ? where id = ? and ? < size";
const char *update_file_content_by_id_sql =
    "update files set content = ?, size = ? where id = ?";

sqlite3_stmt *select_file_by_path_stmt;
sqlite3_stmt *select_path_by_name_stmt;
sqlite3_stmt *select_file_id_by_path_stmt;
sqlite3_stmt *select_path_id_by_path_stmt;
sqlite3_stmt *select_stats_by_parent_id_stmt;
sqlite3_stmt *insert_path_stmt;
sqlite3_stmt *insert_file_stmt;
sqlite3_stmt *select_path_id_stmt;

sqlite3_stmt *delete_path_by_id_stmt;
sqlite3_stmt *delete_file_by_id_stmt;
sqlite3_stmt *increase_file_nlink_by_id_stmt;
sqlite3_stmt *decrease_file_nlink_by_id_stmt;
sqlite3_stmt *select_file_nlink_by_id_stmt;
sqlite3_stmt *select_path_info_by_path_stmt;
sqlite3_stmt *count_dir_items_by_id_stmt;
sqlite3_stmt *update_path_times_by_id_stmt;
sqlite3_stmt *select_file_content_by_id_stmt;
sqlite3_stmt *update_path_name_by_id_stmt;
sqlite3_stmt *update_path_mode_by_id_stmt;
sqlite3_stmt *update_path_owner_by_id_stmt;
sqlite3_stmt *update_file_size_by_id_stmt;
sqlite3_stmt *update_file_content_by_id_stmt;

sqlite3 *db;
char *err_msg;

struct sqlfs_path_info {
    uint64_t id;
    mode_t mode;
    uint64_t file_id;
    uint64_t size;
};

void sqlfs_destroy(void *private_data) { sqlite3_close(db); }

bool is_root_dir(const char *path) {
    if (strcmp(path, "/") == 0) {
        return true;
    } else {
        return false;
    }
}

int sqlfs_getattr(const char *path, struct stat *stat,
                  struct fuse_file_info *fi) {
    if (is_root_dir(path)) {
        stat->st_mode = ROOT_DIR_MODE;
        stat->st_nlink = 1;
        stat->st_uid = getuid();
        stat->st_gid = getgid();
        stat->st_atime = time(NULL);
        stat->st_mtime = time(NULL);
        return 0;
    }
    sqlite3_bind_text(select_path_by_name_stmt, 1, path, -1, NULL);
    int ret = sqlite3_step(select_path_by_name_stmt);
    if (ret == SQLITE_ROW) {
        stat->st_uid = sqlite3_column_int(select_path_by_name_stmt, 0);
        stat->st_gid = sqlite3_column_int(select_path_by_name_stmt, 1);
        stat->st_mode = sqlite3_column_int(select_path_by_name_stmt, 2);
        stat->st_atime = sqlite3_column_int(select_path_by_name_stmt, 3);
        stat->st_mtime = sqlite3_column_int(select_path_by_name_stmt, 4);
        stat->st_ctime = sqlite3_column_int(select_path_by_name_stmt, 5);
        stat->st_size = sqlite3_column_int64(select_path_by_name_stmt, 6);
        stat->st_nlink = sqlite3_column_int(select_path_by_name_stmt, 7);
        ret = OK;
    } else if (ret == SQLITE_DONE) {
        ret = -ENOENT;
    } else {
        printf("sqlfs_getattr(): '%s' sql error %s\n", path,
               sqlite3_errmsg(db));
        ret = -EIO;
    }
    sqlite3_reset(select_path_by_name_stmt);
    return ret;
}
/**
 * @brief find file id by path
 *
 * @param path path to query
 * @param file_id write file id here
 * @return SQLITE_OK on success, SQLITE_DONE on not found
 */
int sqlfs_find_file_id(const char *path, uint64_t *file_id) {
    if (is_root_dir(path)) {
        return SQLITE_DONE;
    }
    sqlite3_bind_text(select_file_id_by_path_stmt, 1, path, -1, NULL);
    int ret = sqlite3_step(select_file_id_by_path_stmt);
    if (ret == SQLITE_ROW) {
        *file_id = sqlite3_column_int64(select_file_id_by_path_stmt, 0);
        ret = SQLITE_OK;
    }
    sqlite3_reset(select_file_id_by_path_stmt);
    return ret;
}

int sqlfs_open(const char *path, struct fuse_file_info *file_info) {
    uint64_t file_id;
    int ret = sqlfs_find_file_id(path, &file_id);
    if (ret == SQLITE_OK) {
        file_info->fh = file_id;
        ret = OK;
    } else if (ret == SQLITE_DONE) {
        printf("sqlfs_open(): not found '%s'\n", path);
        ret = -ENOENT;
    } else {
        printf("sqlfs_open(): error %s\n", sqlite3_errmsg(db));
        ret = -EIO;
    }
    return ret;
}

/**
 * @brief get path id by path name. Root dir '/' id is 0
 *
 * @param path Path to query
 * @param id write path id here
 * @return SQLITE_OK on success, SQLITE_DONE on not found
 */
int sqlfs_find_path_id(const char *path, uint64_t *id) {
    if (is_root_dir(path)) {
        *id = 0;
        return SQLITE_OK;
    }
    sqlite3_bind_text(select_path_id_by_path_stmt, 1, path, -1, NULL);
    int ret = sqlite3_step(select_path_id_by_path_stmt);
    if (ret == SQLITE_ROW) {
        *id = sqlite3_column_int64(select_path_id_by_path_stmt, 0);
        ret = SQLITE_OK;
    }
    sqlite3_reset(select_path_id_by_path_stmt);
    return ret;
}

/**
 * @brief get `path info` by path name.
 *
 * @param path Path to query
 * @param path_info write `path info` here
 * @return SQLITE_OK on success, SQLITE_DONE on not found
 */
int sqlfs_find_path_info(const char *path, struct sqlfs_path_info *path_info) {
    if (is_root_dir(path)) {
        path_info->id = 0;
        path_info->mode = 0;
        path_info->file_id = 0;
        path_info->size = 0;
        return SQLITE_OK;
    }
    sqlite3_bind_text(select_path_info_by_path_stmt, 1, path, -1, NULL);
    int ret = sqlite3_step(select_path_info_by_path_stmt);
    if (ret == SQLITE_ROW) {
        path_info->id = sqlite3_column_int64(select_path_info_by_path_stmt, 0);
        path_info->mode = sqlite3_column_int(select_path_info_by_path_stmt, 1);
        path_info->file_id =
            sqlite3_column_int64(select_path_info_by_path_stmt, 2);
        path_info->size =
            sqlite3_column_int64(select_path_info_by_path_stmt, 3);
        ret = SQLITE_OK;
    }
    sqlite3_reset(select_path_info_by_path_stmt);
    return ret;
}

int sqlfs_opendir(const char *path, struct fuse_file_info *file_info) {
    uint64_t id;
    int ret = sqlfs_find_path_id(path, &id);
    if (ret == SQLITE_OK) {
        file_info->fh = id;
        return 0;
    } else if (ret == SQLITE_DONE) {
        return -ENOENT;
    } else {
        return -EIO;
    }
}

int sqlfs_readdir(const char *path, void *buff, fuse_fill_dir_t filler,
                  off_t offset, struct fuse_file_info *file_info,
                  enum fuse_readdir_flags flags) {
    if (offset == 0) {
        filler(buff, ".", NULL, 0, FUSE_FILL_DIR_PLUS);
        filler(buff, "..", NULL, 0, FUSE_FILL_DIR_PLUS);
    }
    sqlite3_bind_int64(select_stats_by_parent_id_stmt, 1, file_info->fh);
    sqlite3_bind_int64(select_stats_by_parent_id_stmt, 2, 0);
    int ret = sqlite3_step(select_stats_by_parent_id_stmt);
    char path_copy[MAX_PATH_LEN];
    while (ret == SQLITE_ROW) {
        struct stat st;
        const char *p = (const char *)sqlite3_column_text(
            select_stats_by_parent_id_stmt, 0);
        // TODO handle overflow
        strcpy(path_copy, p);
        st.st_uid = sqlite3_column_int(select_stats_by_parent_id_stmt, 1);
        st.st_gid = sqlite3_column_int(select_stats_by_parent_id_stmt, 2);
        st.st_mode = sqlite3_column_int(select_stats_by_parent_id_stmt, 3);
        st.st_atime = sqlite3_column_int64(select_stats_by_parent_id_stmt, 4);
        st.st_mtime = sqlite3_column_int64(select_stats_by_parent_id_stmt, 5);
        st.st_ctime = sqlite3_column_int64(select_stats_by_parent_id_stmt, 6);
        st.st_size = sqlite3_column_int64(select_stats_by_parent_id_stmt, 7);
        st.st_nlink = sqlite3_column_int(select_stats_by_parent_id_stmt, 8);
        filler(buff, basename(path_copy), &st, 0, FUSE_FILL_DIR_PLUS);
        ret = sqlite3_step(select_stats_by_parent_id_stmt);
    }
    if (ret != SQLITE_DONE) {
        printf("sqlfs_readdir(): '%s' path_id: %ld error %s\n", path,
               file_info->fh, sqlite3_errmsg(db));
        ret = -EIO;
    }
    ret = 0;
    sqlite3_reset(select_stats_by_parent_id_stmt);
    return ret;
}
/**
 * @brief Insert a path.
 *
 * @param path path name
 * @param mode
 * @param type, such as S_IFREG, S_IFDIR
 * @param file_id 0 for no file content
 * @return OK if no errors, FUSE negated error otherwise.
 */
int sqlfs_insert_path(const char *path, mode_t mode, int type,
                      uint64_t file_id) {
    if (is_root_dir(path)) {
        return OK;
    }
    uint64_t parent_id;
    char path_copy[MAX_PATH_LEN];
    // TODO handle overflow
    stpcpy(path_copy, path);
    int ret = sqlfs_find_path_id(dirname((char *)path_copy), &parent_id);
    if (ret == SQLITE_OK) {
        // do nothing
    } else if (ret == SQLITE_DONE) {
        return -ENOENT;
    } else {
        return -EIO;
    }
    sqlite3_bind_text(insert_path_stmt, 1, path, -1, NULL);
    sqlite3_bind_int64(insert_path_stmt, 2, parent_id);
    sqlite3_bind_int64(insert_path_stmt, 3, getuid());
    sqlite3_bind_int64(insert_path_stmt, 4, getgid());
    sqlite3_bind_int(insert_path_stmt, 5, mode | type);
    sqlite3_bind_int64(insert_path_stmt, 6, time(NULL));
    sqlite3_bind_int64(insert_path_stmt, 7, time(NULL));
    sqlite3_bind_int64(insert_path_stmt, 8, time(NULL));
    sqlite3_bind_int64(insert_path_stmt, 9, file_id);
    ret = sqlite3_step(insert_path_stmt);
    if (ret == SQLITE_DONE) {
        ret = OK;
    } else {
        printf("sql error in sqlfs_insert_path(): \"%s\" %s\n", path,
               sqlite3_errmsg(db));
        ret = -EIO;
    }
    sqlite3_reset(insert_path_stmt);
    return ret;
}

/**
 * @brief insert an file with content
 *
 * @param content file content
 * @param content_len content length in bytes
 * @param id inserted file id returns here
 * @return int OK if no errors, FUSE negated error otherwise.
 */
int sqlfs_insert_file(const void *content, uint64_t content_len, dev_t dev,
                      uint64_t *id) {
    if (content_len == 0) {
        sqlite3_bind_null(insert_file_stmt, 1);
    } else {
        sqlite3_bind_blob64(insert_file_stmt, 1, content, content_len,
                            SQLITE_STATIC);
    }

    sqlite3_bind_int64(insert_file_stmt, 2, dev);
    sqlite3_bind_int64(insert_file_stmt, 3, content_len);
    int ret = sqlite3_step(insert_file_stmt);
    if (ret == SQLITE_DONE) {
        ret = OK;
    } else {
        printf("sql error in sqlfs_insert_file(): %s\n", sqlite3_errmsg(db));
        ret = -EIO;
    }
    *id = sqlite3_last_insert_rowid(db);
    sqlite3_reset(insert_file_stmt);
    return ret;
}

/**
 * @brief insert an empty file content
 *
 * @param dev linux dev id
 * @param id inserted file id returns here
 * @return int OK if no errors, FUSE negated error otherwise.
 */
int sqlfs_insert_empty_file(dev_t dev, uint64_t *id) {
    return sqlfs_insert_file(NULL, 0, dev, id);
}

int sqlfs_mkdir(const char *path, mode_t mode) {
    return sqlfs_insert_path(path, mode, S_IFDIR, 0);
}

int sqlfs_mknod(const char *path, mode_t mode, dev_t dev) {
    if (is_root_dir(path)) {
        return -EEXIST;
    }
    uint64_t path_id;
    int ret = sqlfs_find_path_id(path, &path_id);
    if (ret == SQLITE_OK) {
        printf("sqlfs_mknod(): file exists '%s'\n", path);
        return -EEXIST;
    } else if (ret == SQLITE_DONE) {
        // do nothing
    } else {
        printf("sqlfs_mknod(): io error '%s'\n", path);
        return -EIO;
    }
    uint64_t file_id;
    ret = sqlfs_insert_empty_file(dev, &file_id);
    if (ret != OK) {
        return ret;
    }

    return sqlfs_insert_path(path, mode, S_IFREG, file_id);
}

int sqlfs_unlink(const char *path) {

    struct sqlfs_path_info path_info;
    int ret = sqlfs_find_path_info(path, &path_info);
    if (ret == OK) {
        // do nothing
    } else if (ret == SQLITE_DONE) {
        printf("sqlfs_unlink(): error no such file '%s'\n", path);
        return -ENOENT;
    } else {
        printf("sqlfs_unlink(): '%s' error %s\n", path, sqlite3_errmsg(db));
        return -EIO;
    }
    if (S_ISDIR(path_info.mode)) {
        printf("sqlfs_unlink(): '%s' is dir\n", path);
        return -EISDIR;
    }
    sqlite3_bind_int64(delete_path_by_id_stmt, 1, path_info.id);
    ret = sqlite3_step(delete_path_by_id_stmt);
    sqlite3_reset(delete_path_by_id_stmt);
    if (ret != SQLITE_DONE) {
        printf("sqlfs_unlink(): '%s' delete path error %s\n", path,
               sqlite3_errmsg(db));
        return -EIO;
    }

    sqlite3_bind_int64(decrease_file_nlink_by_id_stmt, 1, path_info.file_id);
    ret = sqlite3_step(decrease_file_nlink_by_id_stmt);
    sqlite3_reset(decrease_file_nlink_by_id_stmt);
    if (ret != SQLITE_DONE) {
        printf("sqlfs_unlink(): '%s' decrease nlink error %s file_id: %ld, "
               "ret: %d \n",
               path, sqlite3_errmsg(db), path_info.file_id, ret);
        return -EIO;
    }

    sqlite3_bind_int64(select_file_nlink_by_id_stmt, 1, path_info.file_id);
    ret = sqlite3_step(select_file_nlink_by_id_stmt);
    if (ret != SQLITE_ROW) {
        printf("sqlfs_unlink(): '%s' file_id: %ld select nlink error %s\n",
               path, path_info.file_id, sqlite3_errmsg(db));
        return -EIO;
    }
    nlink_t nlink = sqlite3_column_int64(select_file_nlink_by_id_stmt, 0);
    sqlite3_reset(select_file_nlink_by_id_stmt);
    if (nlink == 0) {
        sqlite3_bind_int64(delete_file_by_id_stmt, 1, path_info.file_id);
        ret = sqlite3_step(delete_file_by_id_stmt);
        sqlite3_reset(delete_file_by_id_stmt);
        if (ret != SQLITE_DONE) {
            printf("sqlfs_unlink('%s'): delete file error %s\n", path,
                   sqlite3_errmsg(db));
            return -EIO;
        }
    }
    return OK;
}

int sqlfs_rmdir(const char *path) {
    struct sqlfs_path_info path_info;
    int ret = sqlfs_find_path_info(path, &path_info);
    if (ret == OK) {
        // do nothing
    } else if (ret == SQLITE_DONE) {
        return -ENOENT;
    } else {
        return -EIO;
    }
    if (S_ISREG(path_info.mode)) {
        return -ENOTDIR;
    }
    sqlite3_bind_int64(count_dir_items_by_id_stmt, 1, path_info.id);
    ret = sqlite3_step(count_dir_items_by_id_stmt);
    sqlite3_reset(count_dir_items_by_id_stmt);
    if (ret == SQLITE_ROW) {
        int count = sqlite3_column_int64(count_dir_items_by_id_stmt, 0);
        if (count != 0) {
            return -EPERM;
        }
    } else {
        printf("sqlfs_rmdir(): '%s' count child error %s ret: %d \n", path,
               sqlite3_errmsg(db), ret);
        return -EIO;
    }

    sqlite3_bind_int64(delete_path_by_id_stmt, 1, path_info.id);
    ret = sqlite3_step(delete_path_by_id_stmt);
    sqlite3_reset(delete_path_by_id_stmt);
    if (ret == SQLITE_DONE) {
        ret = OK;
    } else {
        printf("sqlfs_rmdir(): '%s' delete path error %s ret: %d \n", path,
               sqlite3_errmsg(db), ret);
        ret = -EIO;
    }
    return ret;
}

int sqlfs_utimens(const char *path, const struct timespec tv[2],
                  struct fuse_file_info *file_info) {
    struct sqlfs_path_info path_info;
    int ret = sqlfs_find_path_info(path, &path_info);
    if (ret == SQLITE_OK) {
        // do nothing
    } else if (ret == SQLITE_DONE) {
        printf("sqlfs_utimens() '%s' error: no such file\n", path);
        return -ENOENT;
    } else {
        printf("sqlfs_utimens() '%s' error: %s\n", path, sqlite3_errmsg(db));
        return -EIO;
    }
    // TODO fix time
    sqlite3_bind_int64(update_path_times_by_id_stmt, 1, tv[0].tv_nsec * 1000);
    sqlite3_bind_int64(update_path_times_by_id_stmt, 2, tv[1].tv_nsec * 1000);
    sqlite3_bind_int64(update_path_times_by_id_stmt, 3, path_info.id);
    ret = sqlite3_step(update_path_times_by_id_stmt);
    sqlite3_reset(update_path_times_by_id_stmt);
    if (ret == SQLITE_DONE) {
        ret = OK;
    } else {
        printf("sqlfs_utimens('%s') error: %s", path, sqlite3_errmsg(db));
        ret = -EIO;
    }
    return ret;
}

int sqlfs_symlink(const char *old_path, const char *new_path) {
    struct sqlfs_path_info path_info;
    int ret = sqlfs_find_path_info(new_path, &path_info);
    if (ret == SQLITE_OK) {
        return -EEXIST;
    } else if (ret == SQLITE_DONE) {
        // do nothing
    } else {
        printf("sqlfs_symlink() '%s' error: %s\n", new_path,
               sqlite3_errmsg(db));
        return -EIO;
    }

    uint64_t file_id;
    ret = sqlfs_insert_file(old_path, strlen(old_path) + 1, 0, &file_id);
    if (ret != OK) {
        printf("sqlfs_symlink() '%s' insert file error: %s\n", new_path,
               sqlite3_errmsg(db));
        return ret;
    }
    ret = sqlfs_insert_path(new_path, 0755, S_IFLNK, file_id);
    return ret;
}

/**
 * @brief find file content by id
 *
 * @param id
 * @param buff
 * @param buff_size
 * @return OK if find a row
 */
int sqlfs_find_file_content_by_id(u_int64_t id, char *buff, size_t buff_size) {
    sqlite3_blob *blob;
    int ret = sqlite3_blob_open(db, "main", "files", "content", id, 0, &blob);
    if (ret != SQLITE_OK) {
        printf("sqlfs_find_file_content_by_id() blob open error: %s\n",
               sqlite3_errmsg(db));
        sqlite3_blob_close(blob);
        return -EIO;
    }
    uint64_t blob_size = sqlite3_blob_bytes(blob);
    uint64_t max_size = buff_size > blob_size ? blob_size : buff_size;
    ret = sqlite3_blob_read(blob, buff, max_size, 0);
    if (ret != SQLITE_OK) {
        printf("sqlfs_find_file_content_by_id() blob read error: %s\n",
               sqlite3_errmsg(db));
        sqlite3_blob_close(blob);
        return -EIO;
    }
    sqlite3_blob_close(blob);
    return OK;
}

int sqlfs_readlink(const char *path, char *buff, size_t size) {
    struct sqlfs_path_info path_info;
    int ret = sqlfs_find_path_info(path, &path_info);
    if (ret == SQLITE_OK) {
        // do nothing
    } else if (ret == SQLITE_DONE) {
        return -ENOENT;
    } else {
        printf("sqlfs_readlink() '%s' error: %s\n", path, sqlite3_errmsg(db));
        return -EIO;
    }
    return sqlfs_find_file_content_by_id(path_info.file_id, buff, size);
}

int sqlfs_rename(const char *old_path, const char *new_path,
                 unsigned int flags) {
    struct sqlfs_path_info path_info;
    int ret = sqlfs_find_path_info(old_path, &path_info);
    if (ret == SQLITE_OK) {
        // do nothing
    } else if (ret == SQLITE_DONE) {
        printf("sqlfs_rename(): '%s' not found\n", old_path);
        return -ENOENT;
    } else {
        printf("sqlfs_rename(): '%s' to '%s' error: %s\n", old_path, new_path,
               sqlite3_errmsg(db));
        return -EIO;
    }
    struct sqlfs_path_info new_path_info;
    ret = sqlfs_find_path_info(new_path, &new_path_info);
    if (ret == SQLITE_OK) {
        if (S_ISDIR(new_path_info.mode)) {
            printf("sqlfs_rename(): '%s' is dir\n", new_path);
            return -EISDIR;
        }
        ret = sqlfs_unlink(new_path);
        if (ret != OK) {
            printf("sqlfs_rename(): '%s' unlink error\n", new_path);
            return ret;
        }
    } else if (ret == SQLITE_DONE) {
        // do nothings
    } else {
        printf("sqlfs_rename(): '%s' new path error %s\n", new_path,
               sqlite3_errmsg(db));
    }

    sqlite3_bind_text(update_path_name_by_id_stmt, 1, new_path, -1, NULL);
    sqlite3_bind_int64(update_path_name_by_id_stmt, 2, path_info.id);
    ret = sqlite3_step(update_path_name_by_id_stmt);
    if (ret == SQLITE_DONE) {
        ret = OK;
    } else {
        printf("sqlfs_rename() '%s' to '%s' error: %s\n", old_path, new_path,
               sqlite3_errmsg(db));
        ret = -EIO;
    }
    sqlite3_reset(update_path_name_by_id_stmt);
    return ret;
}

int sqlfs_link(const char *old_path, const char *new_path) {
    struct sqlfs_path_info path_info;
    int ret = sqlfs_find_path_info(new_path, &path_info);
    if (ret == SQLITE_OK) {
        printf("sqlfs_rename() '%s' exists\n", new_path);
        return -EEXIST;
    } else if (ret == SQLITE_DONE) {
        // do nothing
    } else {
        printf("sqlfs_rename() '%s' error: %s\n", new_path, sqlite3_errmsg(db));
        return -EIO;
    }
    ret = sqlfs_find_path_info(old_path, &path_info);
    if (ret == SQLITE_OK) {
        // do nothing
    } else if (ret == SQLITE_DONE) {
        printf("sqlfs_rename() '%s' not found\n", old_path);
        return -ENOENT;
    } else {
        printf("sqlfs_rename() '%s' error: %s\n", old_path, sqlite3_errmsg(db));
        return -EIO;
    }
    ret = sqlfs_insert_path(new_path, path_info.mode, path_info.mode & S_IFMT,
                            path_info.file_id);
    if (ret != OK) {
        return ret;
    }

    sqlite3_bind_int64(increase_file_nlink_by_id_stmt, 1, path_info.file_id);

    ret = sqlite3_step(increase_file_nlink_by_id_stmt);
    sqlite3_reset(increase_file_nlink_by_id_stmt);
    if (ret != SQLITE_DONE) {
        printf("sqlfs_rename(): '%s' sql error %s\n", old_path,
               sqlite3_errmsg(db));
        return -EIO;
    } else {
        return OK;
    }
}

int sqlfs_chmod(const char *path, mode_t mode,
                struct fuse_file_info *file_info) {
    struct sqlfs_path_info path_info;
    int ret = sqlfs_find_path_info(path, &path_info);
    if (ret == SQLITE_OK) {
        // do nothing
    } else if (ret == SQLITE_DONE) {
        printf("sqlfs_chmod() '%s' not found", path);
        return -ENOENT;
    } else {
        printf("sqlfs_chmod() '%s' error: %s\n", path, sqlite3_errmsg(db));
        return -EIO;
    }

    sqlite3_bind_int(update_path_mode_by_id_stmt, 1, path_info.mode | mode);
    sqlite3_bind_int64(update_path_mode_by_id_stmt, 2, path_info.id);
    ret = sqlite3_step(update_path_mode_by_id_stmt);
    sqlite3_reset(update_path_mode_by_id_stmt);
    if (ret != SQLITE_DONE) {
        printf("sqlfs_chmod(): '%s' sql error %s\n", path, sqlite3_errmsg(db));
        return -EIO;
    } else {
        return OK;
    }
}

int sqlfs_chown(const char *path, uid_t uid, gid_t gid,
                struct fuse_file_info *file_info) {
    struct sqlfs_path_info path_info;
    int ret = sqlfs_find_path_info(path, &path_info);
    if (ret == SQLITE_OK) {
        // do nothing
    } else if (ret == SQLITE_DONE) {
        printf("sqlfs_chown() '%s' not found", path);
        return -ENOENT;
    } else {
        printf("sqlfs_chown() '%s' error: %s\n", path, sqlite3_errmsg(db));
        return -EIO;
    }

    sqlite3_bind_int(update_path_owner_by_id_stmt, 1, gid);
    sqlite3_bind_int(update_path_owner_by_id_stmt, 2, uid);
    sqlite3_bind_int64(update_path_owner_by_id_stmt, 3, path_info.file_id);
    ret = sqlite3_step(update_path_owner_by_id_stmt);
    sqlite3_reset(update_path_owner_by_id_stmt);
    if (ret != SQLITE_DONE) {
        printf("sqlfs_chown(): '%s' sql error %s\n", path, sqlite3_errmsg(db));
        return -EIO;
    } else {
        return OK;
    }
}

int sqlfs_truncate_file_by_id(u_int64_t file_id, off_t new_size) {
    sqlite3_bind_int(update_file_size_by_id_stmt, 1, new_size);
    sqlite3_bind_int(update_file_size_by_id_stmt, 2, file_id);
    sqlite3_bind_int(update_file_size_by_id_stmt, 3, new_size);
    int ret = sqlite3_step(update_file_size_by_id_stmt);
    sqlite3_reset(update_file_size_by_id_stmt);
    if (ret != SQLITE_DONE) {
        printf("sqlfs_truncate_file_by_id(): file_id: %ld sql error %s\n",
               file_id, sqlite3_errmsg(db));
        return -EIO;
    } else {
        return OK;
    }
}

int sqlfs_truncate(const char *path, off_t new_size,
                   struct fuse_file_info *file_info) {
    struct sqlfs_path_info path_info;
    int ret = sqlfs_find_path_info(path, &path_info);
    if (ret == SQLITE_OK) {
        return sqlfs_truncate_file_by_id(path_info.file_id, new_size);
    } else if (ret == SQLITE_DONE) {
        printf("sqlfs_truncate() '%s' not found", path);
        return -ENOENT;
    } else {
        printf("sqlfs_truncate() '%s' error: %s\n", path, sqlite3_errmsg(db));
        return -EIO;
    }
}

int sqlfs_ftruncate(const char *path, off_t new_size,
                    struct fuse_file_info *file_info) {
    return sqlfs_truncate_file_by_id(file_info->fh, new_size);
}

int sqlfs_write_blob(uint64_t file_id, const char *buff, size_t size,
                     off_t offset) {
    sqlite3_blob *blob;
    int ret =
        sqlite3_blob_open(db, "main", "files", "content", file_id, 1, &blob);
    if (ret != SQLITE_OK) {
        printf("write_blob() open blob error: %s", sqlite3_errmsg(db));
        sqlite3_blob_close(blob);
        return -EIO;
    }

    ret = sqlite3_blob_write(blob, buff, size, offset);
    if (ret != SQLITE_OK) {
        printf("write_blob() write blob error: %s", sqlite3_errmsg(db));
        sqlite3_blob_close(blob);
        return -EIO;
    }

    sqlite3_blob_close(blob);
    return OK;
}

int sqlfs_write_row(struct sqlfs_path_info path_info, const char *buff,
                    size_t size, off_t offset) {

    uint64_t new_size = offset + size;
    char *content_buff = malloc(new_size);
    const char *p = content_buff;
    if (path_info.size > 0) {
        // read old content
        int ret = sqlfs_find_file_content_by_id(path_info.file_id, content_buff,
                                                path_info.size);
        if (ret != OK) {
            printf("sqlfs_write_row() file id not found: %ld\n",
                   path_info.file_id);
            return -EIO;
        }
    }
    memcpy(content_buff + offset, buff, size);
    sqlite3_bind_blob64(update_file_content_by_id_stmt, 1, p, new_size,
                        SQLITE_STATIC);
    sqlite3_bind_int64(update_file_content_by_id_stmt, 2, new_size);
    sqlite3_bind_int64(update_file_content_by_id_stmt, 3, path_info.file_id);
    int ret = sqlite3_step(update_file_content_by_id_stmt);
    if (ret != SQLITE_DONE) {
        printf("sqlfs_write_row(): error %s\n", sqlite3_errmsg(db));
        ret = -EIO;
    } else {
        ret = OK;
    }
    sqlite3_reset(update_file_content_by_id_stmt);
    free(content_buff);
    return ret;
}

int sqlfs_write(const char *path, const char *buff, size_t size, off_t offset,
                struct fuse_file_info *file_info) {
    struct sqlfs_path_info path_info;
    int ret = sqlfs_find_path_info(path, &path_info);
    if (ret == SQLITE_OK) {
        // do nothing
    } else if (ret == SQLITE_DONE) {
        printf("sqlfs_write() '%s' not found", path);
        return -ENOENT;
    } else {
        printf("sqlfs_write() '%s' error: %s\n", path, sqlite3_errmsg(db));
        return -EIO;
    }
    if (offset + size <= path_info.size) {
        ret = sqlfs_write_blob(path_info.file_id, buff, size, offset);
    } else {
        ret = sqlfs_write_row(path_info, buff, size, offset);
    }
    if (ret == OK) {
        return size;
    } else {
        printf("sqlfs_write() error");
        return ret;
    }
}

int sqlfs_read(const char *path, char *buff, size_t size, off_t offset,
               struct fuse_file_info *file_info) {
    sqlite3_blob *blob;
    int ret = sqlite3_blob_open(db, "main", "files", "content", file_info->fh,
                                0, &blob);
    if (ret != SQLITE_OK) {
        printf("sqlfs_read() blob open error: %s\n", sqlite3_errmsg(db));
        sqlite3_blob_close(blob);
        return -EIO;
    }
    uint64_t blob_size = sqlite3_blob_bytes(blob);
    uint64_t max_size = offset + size > blob_size ? blob_size - offset : size;
    ret = sqlite3_blob_read(blob, buff, max_size, offset);
    if (ret != SQLITE_OK) {
        printf("sqlfs_read() blob read error: %s\n", sqlite3_errmsg(db));
        sqlite3_blob_close(blob);
        return -EIO;
    }
    sqlite3_blob_close(blob);
    return max_size;
}

struct fuse_operations operations = {.getattr = sqlfs_getattr,
                                     .destroy = sqlfs_destroy,
                                     .open = sqlfs_open,
                                     .opendir = sqlfs_opendir,
                                     .readdir = sqlfs_readdir,
                                     .mkdir = sqlfs_mkdir,
                                     .mknod = sqlfs_mknod,
                                     .unlink = sqlfs_unlink,
                                     .rmdir = sqlfs_rmdir,
                                     .utimens = sqlfs_utimens,
                                     .symlink = sqlfs_symlink,
                                     .readlink = sqlfs_readlink,
                                     .rename = sqlfs_rename,
                                     .link = sqlfs_link,
                                     .chmod = sqlfs_chmod,
                                     .chown = sqlfs_chown,
                                     .truncate = sqlfs_truncate,
                                     .write = sqlfs_write,
                                     .read = sqlfs_read};

int sqlfs_prepare_stmt(const char *sql, sqlite3_stmt **stmt) {
    return sqlite3_prepare_v2(db, sql, -1, stmt, NULL);
}

int sqlfs_open_db(const char *db_path) { return sqlite3_open(db_path, &db); }

int sqlfs_init_db() {
    int ret = sqlite3_exec(db, create_tables_sql, NULL, NULL, &err_msg);

    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(select_file_by_path_sql,
                                 &select_file_by_path_stmt);
    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(select_path_by_name_sql,
                                 &select_path_by_name_stmt);
    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(select_file_id_by_path_sql,
                                 &select_file_id_by_path_stmt);
    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(select_path_id_by_path_sql,
                                 &select_path_id_by_path_stmt);
    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(select_stats_by_parent_id_sql,
                                 &select_stats_by_parent_id_stmt);
    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(insert_path_sql, &insert_path_stmt);
    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(insert_file_sql, &insert_file_stmt);

    if (ret == SQLITE_OK)
        ret =
            sqlfs_prepare_stmt(delete_path_by_id_sql, &delete_path_by_id_stmt);
    if (ret == SQLITE_OK)
        ret =
            sqlfs_prepare_stmt(delete_file_by_id_sql, &delete_file_by_id_stmt);

    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(select_file_nlink_by_id_sql,
                                 &select_file_nlink_by_id_stmt);
    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(select_path_info_by_path_sql,
                                 &select_path_info_by_path_stmt);
    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(count_dir_items_by_id_sql,
                                 &count_dir_items_by_id_stmt);
    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(update_path_times_by_id_sql,
                                 &update_path_times_by_id_stmt);
    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(increase_file_nlink_by_id_sql,
                                 &increase_file_nlink_by_id_stmt);
    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(decrease_file_nlink_by_id_sql,
                                 &decrease_file_nlink_by_id_stmt);
    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(select_file_content_by_id_sql,
                                 &select_file_content_by_id_stmt);
    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(update_path_name_by_id_sql,
                                 &update_path_name_by_id_stmt);
    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(update_path_mode_by_id_sql,
                                 &update_path_mode_by_id_stmt);
    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(update_path_owner_by_id_sql,
                                 &update_path_owner_by_id_stmt);
    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(update_file_size_by_id_sql,
                                 &update_file_size_by_id_stmt);
    if (ret == SQLITE_OK)
        ret = sqlfs_prepare_stmt(update_file_content_by_id_sql,
                                 &update_file_content_by_id_stmt);
    return ret;
}

struct sqlfs_opts {
    const char *db_path;
    int show_help;
};

static struct fuse_opt opts[] = {
    {"--db %s", offsetof(struct sqlfs_opts, db_path), 0},
    {"--help", offsetof(struct sqlfs_opts, show_help), 1},
    {"-h", offsetof(struct sqlfs_opts, show_help), 1},
};

static void sqlfs_print_help(const char *progname) {
    printf("usage: %s --db=<path> [FUSE options] <mountpoint>\n\n", progname);
    printf("SQLite options:\n"
           "    --db=<path>          path to the SQLite file\n"
           "\n");
}

int main(int argc, char **argv) {
    struct sqlfs_opts sqlfs_opts = {0};
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    int ret = fuse_opt_parse(&args, &sqlfs_opts, opts, NULL);
    if (ret != 0 || !sqlfs_opts.db_path || sqlfs_opts.show_help) {
        sqlfs_print_help(argv[0]);
        assert(fuse_opt_add_arg(&args, "--help") == 0);
        args.argv[0][0] = '\0';
    }

    ret = sqlfs_open_db(sqlfs_opts.db_path);
    if (ret != SQLITE_OK) {
        printf("error when open database %s: %s\n", sqlfs_opts.db_path,
               sqlite3_errmsg(db));
        return ret;
    }
    ret = sqlfs_init_db();
    if (ret != SQLITE_OK) {
        printf("error when init database %s: %s\n", sqlfs_opts.db_path,
               err_msg);
        return ret;
    }

    if (ret != SQLITE_OK) {
        printf("error in sqlite3_prepare_v2(): %s\n", sqlite3_errmsg(db));
        return ret;
    }
    ret = fuse_main(args.argc, args.argv, &operations, NULL);
    if (ret != 0) {
        sqlite3_close(db);
    }
    fuse_opt_free_args(&args);
    return ret;
}
