package com.nablarch.example.redis;

import nablarch.core.repository.initialization.Initializable;

public interface NablarchRedisClient {
    
    String getType();
    
    NablarchRedisCommands<String, String> getCommands();

    <K, V> NablarchRedisCommands<K, V> getCommands(NablarchRedisCodec<K> keyCodec, NablarchRedisCodec<V> valueCodec);
    
    void shutdown();
    
    void initialize();
}
