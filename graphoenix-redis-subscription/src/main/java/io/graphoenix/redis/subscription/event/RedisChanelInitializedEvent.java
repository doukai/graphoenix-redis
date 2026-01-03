package io.graphoenix.redis.subscription.event;

import io.graphoenix.core.handler.DocumentManager;
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;
import io.nozdormu.spi.event.ScopeEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Initialized;
import jakarta.inject.Inject;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.graphoenix.core.event.DocumentInitializedEvent.DOCUMENT_INITIALIZED_SCOPE_EVENT_PRIORITY;
import static io.graphoenix.redis.core.constant.Constants.SUBSCRIPTION_CHANNEL_PREFIX;

@ApplicationScoped
@Initialized(ApplicationScoped.class)
@Priority(RedisChanelInitializedEvent.REDIS_CHANEL_INITIALIZED_SCOPE_EVENT_PRIORITY)
public class RedisChanelInitializedEvent implements ScopeEvent {

    public static final int REDIS_CHANEL_INITIALIZED_SCOPE_EVENT_PRIORITY = DOCUMENT_INITIALIZED_SCOPE_EVENT_PRIORITY + 175;

    private final DocumentManager documentManager;

    private final RedisPubSubReactiveCommands<String, String> pubSubReactiveCommands;

    @Inject
    public RedisChanelInitializedEvent(DocumentManager documentManager, RedisPubSubReactiveCommands<String, String> pubSubReactiveCommands) {
        this.documentManager = documentManager;
        this.pubSubReactiveCommands = pubSubReactiveCommands;
    }

    @Override
    public Mono<Void> fireAsync(Map<String, Object> context) {
        Set<String> channels = documentManager.getDocument().getObjectTypes()
                .filter(objectType -> objectType.isObject() && !documentManager.isOperationType(objectType) && !objectType.isContainer())
                .map(objectType -> SUBSCRIPTION_CHANNEL_PREFIX + "." + objectType.getPackageNameOrError() + "." + objectType.getName())
                .collect(Collectors.toSet());

        return Flux.fromIterable(channels).flatMap(pubSubReactiveCommands::subscribe).then();
    }
}
