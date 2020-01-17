package imd.smartmetropolis.aqueconnect.aqueconnect.processors.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * {@link HandleHDFSFilesImpl}
 */
public class HandleHDFSFilesImpl implements HandleHDFSFiles {

    private static final String hdfsuri = "hdfs://nodemaster:9000";
    private FileSystem fs;

    private void initConfHDFS(){
        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsuri);
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "smartmetropolisgabriel");
        System.setProperty("hadoop.home.dir", "/");
        // Get the filesystem - HDFS
        try {
            fs = FileSystem.get(URI.create(hdfsuri), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void writeFile(String userId, String importSetupName, String fileName, String fileContent) {
        initConfHDFS();
        // Create a path
        Path hdfswritepath = new Path("/user/data/" + userId + '/' + importSetupName + '/' + fileName);
        try {
            boolean overwrite = true;
            FSDataOutputStream outputStream = fs.create(hdfswritepath, overwrite);
            outputStream.writeBytes(fileContent);
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String readFile(String path) {
        Path hdfsreadpath = new Path(path);
        try {
            // Init input stream
            FSDataInputStream inputStream = fs.open(hdfsreadpath);
            String out = IOUtils.toString(inputStream, "UTF-8");
            return out;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}