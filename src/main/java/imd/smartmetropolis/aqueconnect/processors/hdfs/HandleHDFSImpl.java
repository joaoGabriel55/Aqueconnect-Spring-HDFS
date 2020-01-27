package imd.smartmetropolis.aqueconnect.processors.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONArray;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static imd.smartmetropolis.aqueconnect.service.HDFSService.buildHATEOAS;

/**
 * {@link HandleHDFSImpl}
 */
public class HandleHDFSImpl implements HandleHDFS {

    private static final String HDFS_URI = "hdfs://nodemaster:9000";
    private static final String BASE_PATH = "/user/data/";
    public static final String WEB_HDFS_URL = "http://10.7.128.16:9870/webhdfs/v1" + BASE_PATH;
    private static FileSystem fs;

    private static HandleHDFS handleHDFS;

    public static HandleHDFS getInstance() {
        if (handleHDFS == null) {
            handleHDFS = new HandleHDFSImpl();
            initConfHDFS();
        }

        return handleHDFS;
    }

    private static void initConfHDFS() {
        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", HDFS_URI);
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "smartmetropolisgabriel");
        System.setProperty("hadoop.home.dir", "/");
        // Get the filesystem - HDFS
        try {
            fs = FileSystem.get(URI.create(HDFS_URI), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void writeFile(String userId, String path, String fileContent) {
        // Create a path
        String pathWriteFirstTime = BASE_PATH + userId + "/" + path;
        Path hdfsWritePath = new Path(userId != null ? pathWriteFirstTime : path);
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
    public List<ConcurrentHashMap<String, Object>> readFile(String userId, String path) {
        String pathWriteFirstTime = BASE_PATH + userId + "/" + path;
        Path hdfsReadPath = new Path(userId != null ? pathWriteFirstTime : path);
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

    @Override
    public List<Map<String, Object>> listDirectory(String userId, String path) {
        String pathString = path != null ? "/" + path : "";
        String urlPath = WEB_HDFS_URL + userId + pathString + "?op=LISTSTATUS";
        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<Map> responseEntity = restTemplate.getForEntity(urlPath, Map.class);
        Map<String, Object> objectMap = responseEntity.getBody();
        if (objectMap.containsKey("FileStatuses")) {
            Map<String, Object> fileStatuses = (Map<String, Object>) objectMap.get("FileStatuses");
            if (fileStatuses.containsKey("FileStatus")) {
                List<Map<String, Object>> fileStatusList = (List<Map<String, Object>>) fileStatuses.get("FileStatus");
                return fileStatusList.stream().map(elem -> buildHATEOAS(userId, elem)).collect(Collectors.toList());
            }
            return null;
        }
        return null;
    }

    @Override
    public boolean createDirectory(String userId, String name) {
        // Create a path
        Path directory = new Path(BASE_PATH + userId + "/" + name);
        try {
            return fs.mkdirs(directory);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean renameDirectoryOrFile(String userId, String oldName, String newName) {
        // Create a path
        String pathDefault = BASE_PATH + userId + "/";
        Path oldNamePath = new Path(pathDefault + oldName);
        Path newNamePath = new Path(pathDefault + newName);
        try {
            return fs.rename(oldNamePath, newNamePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean removeDirectoryOrFile(String userId, String name) {
        // Create a path
        Path directory = new Path(BASE_PATH + userId + "/" + name);
        try {
            return fs.delete(directory, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

}