package com.nablarch.example.redis.lettuce;

import com.nablarch.example.redis.NablarchRedisClient;
import com.nablarch.example.redis.NablarchRedisCodec;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;

import java.time.Duration;

public class LettuceRedisClient<K, V> implements NablarchRedisClient<K, V> {
    private RedisClient client;
    private StatefulRedisConnection<byte[], byte[]> connection;
    
    private NablarchRedisCodec<K> keyCodec;
    private NablarchRedisCodec<V> valueCodec;

    protected String uri;
    protected long commandTimeout;
    
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

    public void setKeyCodec(NablarchRedisCodec<K> keyCodec) {
        this.keyCodec = keyCodec;
    }

    public void setValueCodec(NablarchRedisCodec<V> valueCodec) {
        this.valueCodec = valueCodec;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public void setCommandTimeout(long commandTimeout) {
        this.commandTimeout = commandTimeout;
    }

    @Override
    public void initialize() {
        client = createClient();
        connection = createConnection();
    }
    
    protected RedisClient createClient() {
        final RedisClient client = RedisClient.create(uri);

        final TimeoutOptions timeoutOptions = TimeoutOptions.builder()
                .fixedTimeout(Duration.ofMillis(commandTimeout))
                .build();

        final ClientOptions clientOptions = ClientOptions.builder()
                .timeoutOptions(timeoutOptions)
                .build();

        client.setOptions(clientOptions);
        
        return client;
    }
    
    protected StatefulRedisConnection<byte[], byte[]> createConnection() {
        return client.connect(new ByteArrayCodec());
    }

    @Override
    public void shutdown() {
        connection.close();
        client.shutdown();
    }
}
