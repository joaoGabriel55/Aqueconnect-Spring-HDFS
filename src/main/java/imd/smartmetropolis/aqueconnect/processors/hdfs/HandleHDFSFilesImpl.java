package imd.smartmetropolis.aqueconnect.processors.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONArray;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * {@link HandleHDFSFilesImpl}
 */
public class HandleHDFSFilesImpl implements HandleHDFSFiles {

    private static final String hdfsuri = "hdfs://nodemaster:9000";
    private FileSystem fs;

    private static HandleHDFSFiles handleHDFSFiles;

    public static HandleHDFSFiles getInstance() {
        if (handleHDFSFiles == null)
            handleHDFSFiles = new HandleHDFSFilesImpl();

        return handleHDFSFiles;
    }

    private void initConfHDFS() {
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
    public void writeFile(String path, String fileContent) {
        initConfHDFS();
        // Create a path
        Path hdfsWritePath = new Path(path);
        try {
            FSDataOutputStream outputStream = fs.create(hdfsWritePath, true);
            outputStream.writeBytes(fileContent);
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ConcurrentHashMap<String, Object>> readFile(String path) {
        initConfHDFS();

        Path hdfsReadPath = new Path(path);
        try {
            // Init input stream
            FSDataInputStream inputStream = fs.open(hdfsReadPath);
            String out = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            JSONArray jsonArray = new JSONArray(out);

            return jsonArray.toList().stream().map(elem -> new ConcurrentHashMap<>(((Map<String, Object>) elem))).collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}