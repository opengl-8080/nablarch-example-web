package com.nablarch.example.redis.lettuce;

import com.nablarch.example.redis.NablarchRedisCodec;
import com.nablarch.example.redis.NablarchRedisCommands;
import io.lettuce.core.api.StatefulRedisConnection;

public class LettuceRedisCommands<K, V> implements NablarchRedisCommands<K, V> {
    private final StatefulRedisConnection<byte[], byte[]> connection;
    private final NablarchRedisCodec<K> keyCodec;
    private final NablarchRedisCodec<V> valueCodec;

    public LettuceRedisCommands(StatefulRedisConnection<byte[], byte[]> connection, NablarchRedisCodec<K> keyCodec, NablarchRedisCodec<V> valueCodec) {
        this.connection = connection;
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
    }

    @Override
    public void set(K key, V value) {
        connection.sync().set(keyCodec.encode(key), valueCodec.encode(value));
    }

    @Override
    public V get(K key) {
        return valueCodec.decode(connection.sync().get(keyCodec.encode(key)));
    }

    @Override
    public void del(K key) {
        connection.sync().del(keyCodec.encode(key));
    }
}
