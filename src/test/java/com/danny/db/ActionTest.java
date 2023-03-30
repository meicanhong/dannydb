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
class ActionTest {
    private Action action;
    @BeforeAll
    void init() throws IOException {
        Files.deleteIfExists(Paths.get("/Users/danny/IdeaProjects/dannydb/data/danny.data"));
        this.action = new Action("/Users/danny/IdeaProjects/dannydb/data");
    }

    @Test
    void get() throws IOException {
        String key = "hello";
        String value = action.get(key);
        System.out.println(value);
        Assertions.assertEquals("danny", value);
    }

    @Test
    void put() throws IOException {
        String key = "hello";
        String value = "danny";
        action.put(key, value);
    }

    @Test
    void delete() throws IOException {
        String key = "delKey";
        String value = "delValue";
        action.put(key, value);
        value = action.get(key);
        Assertions.assertEquals("delValue", value);
        action.delete(key, value);
        value = action.get(key);
        Assertions.assertNull(value);
    }

    @Test
    void merge() throws IOException {
        action.merge();
    }

    @Test
    void pressureTest() throws NoSuchAlgorithmException, IOException {
        String key = "";
        String value = "";
        List<String[]> datas = new ArrayList<String[]>();
        for (int i = 0; i < 5000000; i++) {
            key = RandomHashGenerator.getRandomHash();
            value = RandomHashGenerator.getRandomHash();
            String[] data = {key, value};
            datas.add(data);
        }
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < datas.size(); i++) {
            action.put(datas.get(i)[0], datas.get(i)[1]);
        }
        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;
        System.out.println("Insert 1 Million Records Time elapsed: " + timeElapsed + " ms");

        startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            action.get(datas.get(i)[0]);
        }
        endTime = System.currentTimeMillis();
        timeElapsed = endTime - startTime;
        System.out.println("Get 1 Records Time elapsed: " + timeElapsed + " ms");
    }
}