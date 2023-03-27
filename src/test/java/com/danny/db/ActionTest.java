package com.danny.db;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ActionTest {
    private Action action;
    @BeforeAll
    void init() throws IOException {
        this.action = new Action("/Users/danny/IdeaProjects/bitcask/data");
    }

    @Test
    void get() {

    }

    @Test
    void put() throws IOException {
        String key = "hello";
        String value = "danny";
        action.put(key.getBytes(), value.getBytes());
    }

    @Test
    void delete() {
    }

    @Test
    void merge() {
    }
}