package com.danny.db.util;

import com.github.luben.zstd.Zstd;

import java.io.IOException;

public class CompressionUtil {
    public static byte[] compress(byte[] data){
        return Zstd.compress(data);
    }

    public static byte[] decompress(byte[] data){
        long size = Zstd.decompressedSize(data);
        return Zstd.decompress(data, (int) size);
    }
}
