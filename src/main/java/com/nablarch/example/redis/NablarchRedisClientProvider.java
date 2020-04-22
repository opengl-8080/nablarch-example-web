package com.nablarch.example.redis;

import nablarch.core.log.Logger;
import nablarch.core.log.LoggerManager;
import nablarch.core.repository.initialization.Initializable;

import java.util.List;

public class NablarchRedisClientProvider implements Initializable {
    private static final Logger LOGGER = LoggerManager.get(NablarchRedisClientProvider.class);
    
    private List<NablarchRedisClient> redisClientList;
    private String clientType;
    
    private NablarchRedisClient redisClient;

    @Override
    public void initialize() {
        for (NablarchRedisClient redisClient : redisClientList) {
            if (clientType.equals(redisClient.getType())) {
                this.redisClient = redisClient;
                break;
            }
        }
        LOGGER.logInfo("redisClient=" + redisClient);
        redisClient.initialize();
    }
    
    public NablarchRedisClient getRedisClient() {
        return redisClient;
    }

    public void setRedisClientList(List<NablarchRedisClient> redisClientList) {
        this.redisClientList = redisClientList;
    }

    public void setClientType(String clientType) {
        this.clientType = clientType;
    }
}
