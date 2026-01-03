package io.graphoenix.redis.subscription.handler;

import io.graphoenix.core.config.GraphQLConfig;
import io.graphoenix.core.handler.DocumentManager;
import io.graphoenix.spi.graphql.operation.Operation;
import io.graphoenix.spi.graphql.type.ObjectType;
import io.graphoenix.spi.handler.QueryHandler;
import io.graphoenix.spi.handler.SubscriptionDataListener;
import io.graphoenix.spi.handler.SubscriptionHandler;
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;
import io.nozdormu.spi.context.PublisherBeanContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.literal.NamedLiteral;
import jakarta.enterprise.inject.spi.CDI;
import jakarta.inject.Inject;
import jakarta.json.JsonArray;
import jakarta.json.JsonValue;
import jakarta.json.spi.JsonProvider;
import reactor.core.publisher.Flux;

import java.io.StringReader;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.graphoenix.redis.core.constant.Constants.SUBSCRIPTION_CHANNEL_PREFIX;

@ApplicationScoped
public class RedisSubscriptionHandler implements SubscriptionHandler {

    private final QueryHandler queryHandler;
    private final DocumentManager documentManager;
    private final JsonProvider jsonProvider;
    private final RedisPubSubReactiveCommands<String, String> pubSubReactiveCommands;

    @Inject
    public RedisSubscriptionHandler(
            GraphQLConfig graphQLConfig,
            DocumentManager documentManager,
            JsonProvider jsonProvider,
            RedisPubSubReactiveCommands<String, String> pubSubReactiveCommands
    ) {
        this.documentManager = documentManager;
        this.jsonProvider = jsonProvider;
        this.queryHandler = Optional.ofNullable(graphQLConfig.getDefaultOperationHandlerName())
                .map(name -> CDI.current().select(QueryHandler.class, NamedLiteral.of(name)).get())
                .orElseGet(() -> CDI.current().select(QueryHandler.class).get());
        this.pubSubReactiveCommands = pubSubReactiveCommands;
    }

    @Override
    public Flux<JsonValue> subscription(Operation operation, String token, String operationId) {
        ObjectType operationType = documentManager.getOperationTypeOrError(operation);

        Set<String> channels = operation.getFields().stream()
                .map(field -> operationType.getFieldOrError(field.getName()))
                .map(documentManager::getFieldTypeDefinition)
                .filter(definition -> definition.isObject() && !definition.isContainer())
                .map(definition -> SUBSCRIPTION_CHANNEL_PREFIX + "." + definition.getPackageNameOrError() + "." + definition.getName())
                .collect(Collectors.toSet());

        return PublisherBeanContext.get(SubscriptionDataListener.class)
                .flatMapMany(listener ->
                        Flux.concat(
                                queryHandler.query(operation),
                                pubSubReactiveCommands.observeChannels()
                                        .filter(msg -> channels.contains(msg.getChannel()))
                                        .filter(msg -> msg.getMessage() != null)
                                        .filter(msg -> {
                                                    String channel = msg.getChannel();
                                                    JsonArray mutations = jsonProvider
                                                            .createReader(new StringReader(msg.getMessage()))
                                                            .readArray();
                                                    String typeName = channel.substring(channel.lastIndexOf(".") + 1);
                                                    return listener.changed(typeName, mutations);
                                                }
                                        )
                                        .flatMap(msg -> queryHandler.query(operation))
                        )
                );
    }
}