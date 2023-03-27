package com.danny.db;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ActionTest {
    private Action action;
    @BeforeAll
    void init() throws IOException {
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
}