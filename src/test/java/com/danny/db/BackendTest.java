package com.danny.db;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class BackendTest {
    private Backend backend;
    @BeforeAll
    void init() throws Exception {
        String path = System.getProperty("user.dir");
        Files.deleteIfExists(Paths.get(path + "/data/danny.data"));
        this.backend = new Backend(path + "/data");
    }

    @Test
    void putAndGet() throws Exception {
        byte[] key = "hello".getBytes();
        byte[] value = "danny".getBytes();
        backend.put(key, value);
        Assertions.assertEquals("danny", new String(backend.get(key)));
    }

    @Test
    void delete() throws Exception {
        byte[] key = "delKey".getBytes();
        byte[] value = "delValue".getBytes();
        backend.put(key, value);
        value = backend.get(key);
        Assertions.assertEquals("delValue", new String(value));
        backend.delete(key, value);
        value = backend.get(key);
        Assertions.assertNull(value);
    }

    @Test
    void merge() throws Exception {
        backend.merge();
    }

    @Test
    void pressureTest() throws NoSuchAlgorithmException, IOException, ExecutionException, InterruptedException {
        String key = "";
        String value = "";
        List<String[]> datas = new ArrayList<String[]>();
        for (int i = 0; i < 1; i++) {
            key = RandomHashGenerator.getRandomHash();
            value = RandomHashGenerator.getRandomHash();
            String[] data = {key, value};
            datas.add(data);
        }
        long startTime = System.currentTimeMillis();
        byte[] bytes = datas.get(0)[0].getBytes();
        for (int i = 0; i < datas.size(); i++) {
            backend.put(datas.get(i)[0].getBytes(), datas.get(i)[1].getBytes());
        }
        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;
        System.out.println("Insert 1 Million Records Time elapsed: " + timeElapsed + " ms");

        startTime = System.currentTimeMillis();
        for (int i = 0; i < 1; i++) {
            byte[] result = backend.get(datas.get(i)[0].getBytes());
            Assertions.assertEquals(datas.get(i)[1], new String(result));
        }
        endTime = System.currentTimeMillis();
        timeElapsed = endTime - startTime;
        System.out.println("Get 500000 Records Time elapsed: " + timeElapsed + " ms");
    }
}