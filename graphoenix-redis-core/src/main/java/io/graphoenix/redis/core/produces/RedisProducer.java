package io.graphoenix.redis.core.produces;

import io.graphoenix.redis.core.config.RedisConfig;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Provider;

import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

@ApplicationScoped
public class RedisProducer {

    private final Provider<RedisClient> redisClientProvider;
    private final RedisConfig redisConfig;

    @Inject
    public RedisProducer(Provider<RedisClient> redisClientProvider, RedisConfig redisConfig) {
        this.redisClientProvider = redisClientProvider;
        this.redisConfig = redisConfig;
    }

    @Produces
    @ApplicationScoped
    public RedisClient redisClient() {
        if (redisConfig.getUri() != null) {
            return RedisClient.create(redisConfig.getUri());
        }
        RedisURI redisURI = new RedisURI();
        redisURI.setHost(redisConfig.getHost());
        redisURI.setPort(redisConfig.getPort());
        if (redisConfig.getDatabase() != null) {
            redisURI.setDatabase(redisConfig.getDatabase());
        }
        if (redisConfig.getUsername() != null) {
            redisURI.setAuthentication(redisConfig.getUsername(), redisConfig.getPassword());
        } else if (redisConfig.getPassword() != null) {
            redisURI.setAuthentication(redisConfig.getPassword());
        }
        if (redisConfig.getSentinelMasterId() != null) {
            redisURI.setSentinelMasterId(redisConfig.getSentinelMasterId());
        }
        if (redisConfig.getClientName() != null) {
            redisURI.setClientName(redisConfig.getClientName());
        }
        if (redisConfig.getLibraryName() != null) {
            redisURI.setLibraryName(redisConfig.getLibraryName());
        }
        if (redisConfig.getLibraryVersion() != null) {
            redisURI.setLibraryVersion(redisConfig.getLibraryVersion());
        }
        if (redisConfig.getTimeout() != null) {
            redisURI.setTimeout(Duration.of(redisConfig.getTimeout(), SECONDS));
        }
        return RedisClient.create(redisURI);
    }

    @Produces
    @ApplicationScoped
    public RedisPubSubReactiveCommands<String, String> redisPubSubReactiveCommands() {
        return redisClientProvider.get().connectPubSub().reactive();
    }

    @Produces
    @ApplicationScoped
    public RedisReactiveCommands<String, String> reactiveCommands() {
        return redisClientProvider.get().connect().reactive();
    }
}
