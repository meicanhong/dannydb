package com.danny.db;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Entry implements Serializable {
    public static final int ENTRY_HEADER_SIZE = 10;
    public static final short PUT = 0;
    public static final short DEL = 1;
    private byte[] key;
    private byte[] value;
    private int keySize;
    private int valueSize;
    private short mark;

    public Entry(byte[] key, byte[] value, short mark) {
        this.key = key;
        this.value = value;
        this.keySize = key.length;
        this.valueSize = value.length;
        this.mark = mark;
    }

    public byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(getSize()).order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(keySize);
        buffer.putInt(valueSize);
        buffer.putShort(mark);
        buffer.put(key);
        buffer.put(value);
        return buffer.array();
    }

    public static Entry decode(byte[] buffer) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.BIG_ENDIAN);
        int keySize = byteBuffer.getInt();
        int valueSize = byteBuffer.getInt();
        short mark = byteBuffer.getShort();
        byte[] key = new byte[keySize];
        byteBuffer.get(key);
        byte[] value = new byte[valueSize];
        byteBuffer.get(value);
        return new Entry(key, value, mark);
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public short getMark() {
        return mark;
    }

    public int getKeySize() {
        return this.keySize;
    }

    public int getValueSize() {
        return this.valueSize;
    }

    public int getSize() {
        return keySize + valueSize + ENTRY_HEADER_SIZE;
    }
}
