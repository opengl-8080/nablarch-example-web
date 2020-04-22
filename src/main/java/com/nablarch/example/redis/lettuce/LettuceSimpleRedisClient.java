package com.nablarch.example.redis.lettuce;

import com.nablarch.example.redis.NablarchRedisCodec;
import com.nablarch.example.redis.NablarchRedisCommands;
import com.nablarch.example.redis.NablarchRedisStringCodec;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;

import java.time.Duration;

public class LettuceSimpleRedisClient extends AbstractLettuceRedisClient {
    
    private RedisClient client;
    private StatefulRedisConnection<byte[], byte[]> connection;

    protected String uri;
    protected long commandTimeout;

    public LettuceSimpleRedisClient() {
        super("simple");
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

    @Override
    public String toString() {
        return "LettuceSimpleRedisClient{" +
                ", uri='" + uri + '\'' +
                ", commandTimeout=" + commandTimeout +
                '}';
    }
}
