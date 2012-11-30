-- --------------------------------------------------------------------
--
-- filesystem.sql
--
-- User defined filesystems
--
--
-- --------------------------------------------------------------------

------------------------------------------------------------------
-- Create HDFS filesystem
------------------------------------------------------------------

CREATE FILESYSTEM HDFS
(
    gpfs_libfile = "$libdir/gpfshdfs.so",
    gpfs_connect = "gpfs_hdfs_connect",
    gpfs_disconnect = "gpfs_hdfs_disconnect",
    gpfs_open = "gpfs_hdfs_openfile",
    gpfs_close = "gpfs_hdfs_closefile",
    gpfs_seek = "gpfs_hdfs_seek",
    gpfs_tell = "gpfs_hdfs_tell",
    gpfs_read = "gpfs_hdfs_read",
    gpfs_write = "gpfs_hdfs_write",
    gpfs_flush = "gpfs_hdfs_sync",
    gpfs_delete = "gpfs_hdfs_delete",
    gpfs_chmod = "gpfs_hdfs_chmod",
    gpfs_mkdir = "gpfs_hdfs_createdirectory",
    gpfs_truncate = "gpfs_hdfs_truncate",
    gpfs_getpathinfo = "gpfs_hdfs_getpathinfo",
    gpfs_freefileinfo = "gpfs_hdfs_freefileinfo"
);

