package com.nablarch.example.redis;

import nablarch.core.repository.initialization.Initializable;

public interface NablarchRedisClient<K, V> extends Initializable {
    void set(K key, V value);
    V get(K key);
    void del(K key);
    
    void shutdown();
}
