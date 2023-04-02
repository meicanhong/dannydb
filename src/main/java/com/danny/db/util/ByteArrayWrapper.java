package com.danny.db.util;
import java.util.Arrays;
public class ByteArrayWrapper {
    private final byte[] array;

    public ByteArrayWrapper(byte[] array) {
        this.array = array;
    }

    public byte[] getArray() {
        return array;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ByteArrayWrapper that = (ByteArrayWrapper) o;
        return Arrays.equals(array, that.array);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(array);
    }
}

