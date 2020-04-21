package com.nablarch.example.redis;

public interface NablarchRedisCommands<K, V> {
    
    void set(K key, V value);
    V get(K key);
    void del(K key);
}
