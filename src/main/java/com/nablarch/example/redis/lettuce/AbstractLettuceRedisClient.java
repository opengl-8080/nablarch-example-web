package com.nablarch.example.redis.lettuce;

import com.nablarch.example.redis.NablarchRedisClient;
import com.nablarch.example.redis.NablarchRedisStringCodec;

public abstract class AbstractLettuceRedisClient implements NablarchRedisClient {
    protected static final NablarchRedisStringCodec DEFAULT_STRING_CODEC = new NablarchRedisStringCodec("UTF-8");
    private final String name;

    protected AbstractLettuceRedisClient(String name) {
        this.name = name;
    }

    @Override
    public String getType() {
        return name;
    }
}
