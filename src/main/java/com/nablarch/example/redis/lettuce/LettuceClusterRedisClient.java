package com.nablarch.example.redis.lettuce;

import com.nablarch.example.redis.NablarchRedisCodec;
import com.nablarch.example.redis.NablarchRedisCommands;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.codec.ByteArrayCodec;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

public class LettuceClusterRedisClient extends AbstractLettuceRedisClient {
    private RedisClusterClient client;
    private StatefulRedisClusterConnection<byte[], byte[]> connection;

    protected List<String> nodeUriList;
    protected String nodeUris;
    protected long commandTimeout = 60_000L;
    protected long topologyRefreshDuration = 30_000L;
    protected ReadFrom readFrom = ReadFrom.REPLICA;

    public LettuceClusterRedisClient() {
        super("cluster");
    }

    @Override
    public NablarchRedisCommands<String, String> getCommands() {
        return new LettuceClusterRedisCommands<>(connection, DEFAULT_STRING_CODEC, DEFAULT_STRING_CODEC);
    }

    @Override
    public <K, V> NablarchRedisCommands<K, V> getCommands(NablarchRedisCodec<K> keyCodec, NablarchRedisCodec<V> valueCodec) {
        return new LettuceClusterRedisCommands<>(connection, keyCodec, valueCodec);
    }

    public void setNodeUriList(List<String> nodeUriList) {
        this.nodeUriList = nodeUriList;
    }

    public void setCommandTimeout(long commandTimeout) {
        this.commandTimeout = commandTimeout;
    }

    public void setTopologyRefreshDuration(long topologyRefreshDuration) {
        this.topologyRefreshDuration = topologyRefreshDuration;
    }

    public void setReadFrom(ReadFrom readFrom) {
        this.readFrom = readFrom;
    }

    @Override
    public void initialize() {
        client = createClient();
        connection = createConnection();
    }
    
    protected RedisClusterClient createClient() {
        RedisClusterClient client = RedisClusterClient.create(
                nodeUriList.stream().map(RedisURI::create).collect(Collectors.toSet()));

        final ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
                .enablePeriodicRefresh(Duration.ofMillis(topologyRefreshDuration))
                .enableAllAdaptiveRefreshTriggers()
                .build();

        final TimeoutOptions timeoutOptions = TimeoutOptions.builder().fixedTimeout(Duration.ofMillis(commandTimeout)).build();

        final ClusterClientOptions clientOptions = ClusterClientOptions.builder()
                .timeoutOptions(timeoutOptions)
                .topologyRefreshOptions(topologyRefreshOptions)
                .build();

        client.setOptions(clientOptions);
        return client;
    }
    
    protected StatefulRedisClusterConnection<byte[], byte[]> createConnection() {
        StatefulRedisClusterConnection<byte[], byte[]> connection = client.connect(new ByteArrayCodec());
        connection.setReadFrom(readFrom);
        return connection;
    }

    @Override
    public void shutdown() {
        connection.close();
        client.shutdown();
    }
}
