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
        String result = new String(backend.get(key));
        System.out.println(result);
        Assertions.assertEquals("danny", result);
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
        putAndGet();
        delete();
        backend.merge();
    }

    @Test
    void pressureTest() throws NoSuchAlgorithmException, IOException, InterruptedException {
        String key = "";
        String value = "";
        int size = 1000000;
        List<byte[][]> datas = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            key = RandomHashGenerator.getRandomHash();
            value = RandomHashGenerator.getRandomHash();
            byte[][] data = {key.getBytes(), value.getBytes()};
            datas.add(data);
        }
        long startTime = System.currentTimeMillis();
        backend.putBatch(datas);
        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;
        System.out.println("Insert "+ size +" Records Time elapsed: " + timeElapsed + " ms");

        startTime = System.currentTimeMillis();
        for (int i = 0; i < datas.size(); i++) {
            byte[] result = backend.get(datas.get(i)[0]);
            Assertions.assertEquals(new String(datas.get(i)[1]), new String(result));
        }
        endTime = System.currentTimeMillis();
        timeElapsed = endTime - startTime;
        System.out.println("Got "+ size + " Records Time elapsed: " + timeElapsed + " ms");
    }
}