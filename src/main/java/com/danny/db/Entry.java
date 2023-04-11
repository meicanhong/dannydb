package com.danny.db;

import com.danny.db.util.CompressionUtil;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Entry implements Serializable {
    public static final int ENTRY_HEADER_SIZE = 10;
    public static final short PUT = 0;
    public static final short DEL = 1;
    private byte[] key;
    private byte[] value;

    private int diskSize;
    private int keySize;
    private int valueSize;
    private short mark;

    public Entry(byte[] key, byte[] value, short mark) {
        this.key = key;
        this.value = value;
        this.keySize = key.length;
        this.valueSize = value.length;
        this.mark = mark;
        this.diskSize = CompressionUtil.compress(key).length + CompressionUtil.compress(value).length + ENTRY_HEADER_SIZE;
    }

    private Entry(int keySize, int valueSize, short mark) {
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.diskSize = keySize + valueSize + ENTRY_HEADER_SIZE;
        this.mark = mark;
    }

    public ByteBuffer encode() {
        ByteBuffer buffer = ByteBuffer.allocate(getDiskSize()).order(ByteOrder.BIG_ENDIAN);
        byte[] compress_key = CompressionUtil.compress(this.key);
        byte[] compress_value = CompressionUtil.compress(this.value);
        buffer.putInt(compress_key.length);
        buffer.putInt(compress_value.length);
        buffer.putShort(mark);
        buffer.put(compress_key);
        buffer.put(compress_value);
        buffer.flip();
        return buffer;
    }

    public static Entry decode(byte[] buffer) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.BIG_ENDIAN);
        int keySize = byteBuffer.getInt();
        int valueSize = byteBuffer.getInt();
        short mark = byteBuffer.getShort();
        return new Entry(keySize, valueSize, mark);
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

    public int getDiskSize() {
        return diskSize;
    }
}
