package com.nablarch.example.app.web.action;

import com.nablarch.example.redis.NablarchRedisClient;
import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.repository.SystemRepository;
import nablarch.fw.ExecutionContext;
import nablarch.fw.web.HttpRequest;
import nablarch.fw.web.HttpResponse;

import java.nio.charset.StandardCharsets;

public class RedisTestAction {
    private static final Logger LOGGER = LoggerManager.get(RedisTestAction.class);
    
    public HttpResponse index(HttpRequest request, ExecutionContext context) {
        final NablarchRedisClient<String, byte[]> redisClient = SystemRepository.get("redisClient");
        redisClient.set("message", "おはようRedis!!".getBytes(StandardCharsets.UTF_8));
        final byte[] foo = redisClient.get("foo");
        if (foo != null) {
            LOGGER.logInfo("@@@@ foo = " + new String(foo, StandardCharsets.UTF_8));
        } else {
            LOGGER.logInfo("@@@@ foo is null.");
        }

        return new HttpResponse("/WEB-INF/view/redis/index.jsp");
    }
}
