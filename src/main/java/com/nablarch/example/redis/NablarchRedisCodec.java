package com.nablarch.example.redis;

public interface NablarchRedisCodec<T> {
    byte[] encode(T target);
    T decode(byte[] bytes);
}
