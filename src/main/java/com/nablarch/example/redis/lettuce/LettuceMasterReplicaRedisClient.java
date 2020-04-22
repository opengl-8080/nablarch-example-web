package com.nablarch.example.redis.lettuce;

import com.nablarch.example.redis.NablarchRedisCodec;
import com.nablarch.example.redis.NablarchRedisCommands;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;

import java.time.Duration;

public class LettuceMasterReplicaRedisClient extends AbstractLettuceRedisClient {
    private RedisClient client;
    private StatefulRedisMasterReplicaConnection<byte[], byte[]> connection;
    
    protected String uri;
    protected long commandTimeout = 60_000L;
    protected ReadFrom readFrom = ReadFrom.REPLICA;

    public LettuceMasterReplicaRedisClient() {
        super("masterReplica");
    }

    @Override
    public NablarchRedisCommands<String, String> getCommands() {
        return new LettuceRedisCommands<>(connection, DEFAULT_STRING_CODEC, DEFAULT_STRING_CODEC);
    }

    @Override
    public <K, V> NablarchRedisCommands<K, V> getCommands(NablarchRedisCodec<K> keyCodec, NablarchRedisCodec<V> valueCodec) {
        return new LettuceRedisCommands<>(connection, keyCodec, valueCodec);
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
