package imd.smartmetropolis.aqueconnect.services;

import imd.smartmetropolis.aqueconnect.AbstractTest;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HDFSServiceTest extends AbstractTest {

    @Autowired
    private FileService service;

    @Override
    @Before
    public void setUp() {
        super.setUp();
    }

    @Test
    public void readFile() throws IOException {
        Path path = new Path("/user/data/cf7dbe44-30eb-4145-8636-9ce0bc49e0ed/MATRICULA_SUDESTE.csv");
        String result = service.readFileAsString(path);
        assertFalse(result.isEmpty());
    }

    @Test
    public void listDirectory() throws Exception {
        List<Map<String, Object>> jsonObject = service.listDirectory(
                "cf7dbe44-30eb-4145-8636-9ce0bc49e0ed",
                "testhdfs1"
        );
        assertFalse(jsonObject.isEmpty());
    }

    @Test
    public void createDirectorySuccess() throws IOException {
        boolean created = service.createDirectory("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1");
        assertTrue(created);
        assertTrue(service.removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1"));
    }

    @Test
    public void renameDirectorySuccess() throws IOException {
        service.createDirectory("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1");
        boolean renamed = service.renameDirectoryOrFile(
                "cf7dbe44-30eb-4145-8636-9ce0bc49e0ed",
                "test1",
                "test2"
        );
        assertTrue(renamed);
        assertTrue(service.removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test2"));
    }

    @Test
    public void renameFileSuccess() throws IOException {
        service.writeFileString("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1/text.txt", "Hello");
        boolean renamed = service.renameDirectoryOrFile(
                "cf7dbe44-30eb-4145-8636-9ce0bc49e0ed",
                "test1/text.txt",
                "test1/text2.txt"
        );
        assertTrue(renamed);
        assertTrue(service.removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1"));
    }

    @Test
    public void removeDirectorySuccess() throws IOException {
        service.createDirectory("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1");
        boolean removed = service.removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1");
        assertTrue(removed);
    }

    @Test
    public void removeFileSuccess() throws IOException {
        service.writeFileString("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1/text.txt", "Hello");
        boolean removed = service.removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1/text.txt");
        assertTrue(removed);
        assertTrue(service.removeDirectoryOrFile("cf7dbe44-30eb-4145-8636-9ce0bc49e0ed", "test1"));
    }

    @Test
    public void getFileResourceTest() throws IOException {
        service.writeFileString("u1", "/p1/test.csv", "Hello");
        InputStreamResource resource = service.getFileResource("u1", "/p1/test.csv");
        long size = resource.contentLength();
        assertTrue(size == 5);
        service.removeDirectoryOrFile("u1", "");
    }

}
