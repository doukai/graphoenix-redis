package io.graphoenix.redis.core.config;

import com.typesafe.config.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperties;

@ConfigProperties(prefix = "redis")
public class RedisConfig {

    @Optional
    private String uri;

    @Optional
    private String host = "localhost";

    @Optional
    private Integer port = 6379;

    @Optional
    private Integer database;

    @Optional
    private String username;

    @Optional
    private String password;

    @Optional
    private String sentinelMasterId;

    @Optional
    private String clientName;

    @Optional
    private String libraryName;

    @Optional
    private String libraryVersion;

    @Optional
    private Integer timeout;

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public Integer getDatabase() {
        return database;
    }

    public void setDatabase(Integer database) {
        this.database = database;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSentinelMasterId() {
        return sentinelMasterId;
    }

    public void setSentinelMasterId(String sentinelMasterId) {
        this.sentinelMasterId = sentinelMasterId;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public String getLibraryName() {
        return libraryName;
    }

    public void setLibraryName(String libraryName) {
        this.libraryName = libraryName;
    }

    public String getLibraryVersion() {
        return libraryVersion;
    }

    public void setLibraryVersion(String libraryVersion) {
        this.libraryVersion = libraryVersion;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }
}
