package com.nablarch.example.app.web.action;

import com.nablarch.example.redis.NablarchRedisByteArrayCodec;
import com.nablarch.example.redis.NablarchRedisClient;
import com.nablarch.example.redis.NablarchRedisClientProvider;
import com.nablarch.example.redis.NablarchRedisCommands;
import com.nablarch.example.redis.NablarchRedisStringCodec;
import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.repository.SystemRepository;
import nablarch.fw.ExecutionContext;
import nablarch.fw.web.HttpRequest;
import nablarch.fw.web.HttpResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class RedisTestAction {
    private static final Logger LOGGER = LoggerManager.get(RedisTestAction.class);
    private static AtomicInteger counter = new AtomicInteger(0);
    
    public HttpResponse index(HttpRequest request, ExecutionContext context) {
        final NablarchRedisClientProvider provider = SystemRepository.get("redisClientProvider");
        final NablarchRedisClient redisClient = provider.getRedisClient();
        final NablarchRedisCommands<String, String> commands = redisClient.getCommands();

        final String message = commands.get("message");
        LOGGER.logInfo("@@@ message=" + message);
        
        commands.set("message", "おはようRedis!! at " + LocalDateTime.now());


        final NablarchRedisCommands<String, byte[]> commands2 = redisClient.getCommands(new NablarchRedisStringCodec("UTF-8"), new NablarchRedisByteArrayCodec());
        final byte[] bytes = commands2.get("list");
        ArrayList<Integer> list = bytes == null ? new ArrayList<>() : deserialize(bytes);
        LOGGER.logInfo("@@@ list=" + list);
        list.add(counter.getAndIncrement());
        commands2.set("list", serialize(list));

        return new HttpResponse("/WEB-INF/view/redis/index.jsp");
    }
    
    private ArrayList<Integer> deserialize(byte[] bytes) {
        try (ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            //noinspection unchecked
            return (ArrayList<Integer>) objectInputStream.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    
    private byte[] serialize(Serializable object) {
        try (final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             final ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);) {
            objectOutputStream.writeObject(object);
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
