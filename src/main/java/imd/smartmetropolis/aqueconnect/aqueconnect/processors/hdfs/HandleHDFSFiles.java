package imd.smartmetropolis.aqueconnect.aqueconnect.processors.hdfs;

/**
 * HandleHDFSFiles
 */
public interface HandleHDFSFiles {

    void writeFile(String userId, String importSetupName, String fileName, String fileContent);

    String readFile(String path);
}