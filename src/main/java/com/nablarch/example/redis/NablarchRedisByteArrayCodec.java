package com.nablarch.example.redis;

public class NablarchRedisByteArrayCodec implements NablarchRedisCodec<byte[]> {
    @Override
    public byte[] encode(byte[] target) {
        return target;
    }

    @Override
    public byte[] decode(byte[] bytes) {
        return bytes;
    }
}
