package com.nablarch.example.redis.lettuce;

import com.nablarch.example.redis.NablarchRedisCodec;
import com.nablarch.example.redis.NablarchRedisCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

public class LettuceClusterRedisCommands<K, V> implements NablarchRedisCommands<K, V> {
    private final StatefulRedisClusterConnection<byte[], byte[]> connection;
    private final NablarchRedisCodec<K> keyCodec;
    private final NablarchRedisCodec<V> valueCodec;

    public LettuceClusterRedisCommands(StatefulRedisClusterConnection<byte[], byte[]> connection, NablarchRedisCodec<K> keyCodec, NablarchRedisCodec<V> valueCodec) {
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
        this.connection = connection;
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
