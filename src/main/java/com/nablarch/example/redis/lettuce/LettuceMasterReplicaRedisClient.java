package com.nablarch.example.redis.lettuce;

import com.nablarch.example.redis.NablarchRedisClient;
import com.nablarch.example.redis.NablarchRedisCodec;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;

import java.time.Duration;

public class LettuceMasterReplicaRedisClient<K, V> implements NablarchRedisClient<K, V> {
    private RedisClient client;
    private StatefulRedisMasterReplicaConnection<byte[], byte[]> connection;

    private NablarchRedisCodec<K> keyCodec;
    private NablarchRedisCodec<V> valueCodec;
    
    protected String uri;
    protected long commandTimeout = 60_000L;
    protected ReadFrom readFrom = ReadFrom.REPLICA;

    
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

    public void setReadFrom(ReadFrom readFrom) {
        this.readFrom = readFrom;
    }

    
    @Override
    public void initialize() {
        client = createClient();
        connection = createConnection();
    }
    
    protected RedisClient createClient() {
        RedisClient client = RedisClient.create();

        final TimeoutOptions timeoutOptions = TimeoutOptions.builder()
                .fixedTimeout(Duration.ofMillis(commandTimeout))
                .build();

        final ClientOptions clientOptions = ClientOptions.builder()
                .timeoutOptions(timeoutOptions)
                .build();

        client.setOptions(clientOptions);
        return client;
    }
    
    protected StatefulRedisMasterReplicaConnection<byte[], byte[]> createConnection() {
        StatefulRedisMasterReplicaConnection<byte[], byte[]> connection =
                MasterReplica.connect(client, new ByteArrayCodec(), RedisURI.create(uri));
        connection.setReadFrom(readFrom);
        return connection;
    }

    @Override
    public void shutdown() {
        connection.close();
        client.shutdown();
    }
}
