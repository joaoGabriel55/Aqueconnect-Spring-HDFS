package imd.smartmetropolis.aqueconnect.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

import static imd.smartmetropolis.aqueconnect.config.PropertiesParams.HDFS_URI;
import static imd.smartmetropolis.aqueconnect.config.PropertiesParams.USER_NAME_HDFS;

public class HdfsUtils {

    public static FileSystem initConfHDFS() {
        // ====== Init HDFS File System Object
        FileSystem fs = null;
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", HDFS_URI);
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", USER_NAME_HDFS);
        System.setProperty("hadoop.home.dir", "/");
        // Get the filesystem - HDFS
        try {
            fs = FileSystem.get(URI.create(HDFS_URI), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fs;
    }
}
