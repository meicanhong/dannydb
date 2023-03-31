package com.danny.db.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.ConcurrentHashMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ByteArrayWrapperTest {

    @Test
    void test() {
        ConcurrentHashMap<ByteArrayWrapper, String> map = new ConcurrentHashMap<>();
        byte[] key = "aaa".getBytes();
        map.put(new ByteArrayWrapper(key), "bbb");
        String value = map.get(new ByteArrayWrapper("aaa".getBytes()));
        Assertions.assertEquals("bbb", value);
    }
}