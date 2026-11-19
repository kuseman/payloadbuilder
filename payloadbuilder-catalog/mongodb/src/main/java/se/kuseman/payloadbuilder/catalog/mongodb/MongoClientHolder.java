package se.kuseman.payloadbuilder.catalog.mongodb;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.catalog.CredentialsException;

/**
 * Caches one {@link MongoClient} per connection string + authentication identity + timeout configuration.
 *
 * <pre>
 * A {@link MongoClient} pools its own connections and is thread safe, but connection strings are often ephemeral
 * (ie. pointing at short lived docker containers) so idle clients are closed and evicted by a housekeeping task,
 * mirroring the JdbcCatalog's pooled datasource housekeeping.
 *
 * NOTE! The cache key includes the username, a hash of the password and the effective timeouts (not the connection string alone). This matters
 * because a MongoClient/MongoCredential is immutable once built - unlike JdbcCatalog's HikariDataSource, there is no way to swap credentials or
 * timeouts on an already constructed client. Keying only by connection string would let two different users of the same URI (or a rotated password,
 * or a different timeout override) silently keep reusing whichever client happened to be built first. Since the key changes when any of those
 * change, a change simply results in a new cache entry - the old one is left untouched and ages out via the housekeeping task above.
 *
 * NOTE! The driver defaults socketTimeout (read timeout on an established connection) to 0, ie. NO timeout - a firewall that silently drops
 * packets (rather than actively refusing the connection) to an unreachable replica set member can then hang a query forever with no exception
 * ever thrown. Bounded (but overridable) defaults are used here instead so such failures show up as a clear, bounded exception.
 * </pre>
 */
class MongoClientHolder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoClientHolder.class);
    private static final long IDLE_MINUTES = 10;
    private static final int DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
    private static final int DEFAULT_SOCKET_TIMEOUT_MS = 30_000;
    private static final int DEFAULT_SERVER_SELECTION_TIMEOUT_MS = 30_000;

    private final Map<String, ClientHolder> clientByKey = new ConcurrentHashMap<>();
    private final ScheduledFuture<?> houseKeepingFuture;

    MongoClientHolder()
    {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, BasicThreadFactory.builder()
                .daemon(true)
                .namingPattern("MongoCatalog-Client-housekeeper-%d")
                .build());
        houseKeepingFuture = scheduler.scheduleAtFixedRate(houseKeepingRunnable, IDLE_MINUTES, IDLE_MINUTES, TimeUnit.MINUTES);
    }

    MongoClient getClient(IQuerySession session, String catalogAlias)
    {
        String connectionString = getConnectionString(session, catalogAlias);
        String username = session.getCatalogProperty(catalogAlias, MongoCatalog.AUTH_USERNAME_KEY)
                .valueAsString(0);
        String password = null;
        String authDatabase = null;

        if (isNotBlank(username))
        {
            password = session.getCatalogProperty(catalogAlias, MongoCatalog.AUTH_PASSWORD_KEY)
                    .valueAsString(0);
            if (isBlank(password))
            {
                throw new CredentialsException(catalogAlias, "Missing " + MongoCatalog.AUTH_PASSWORD_KEY + " in catalog properties for catalog alias: " + catalogAlias);
            }
            authDatabase = session.getCatalogProperty(catalogAlias, MongoCatalog.AUTH_DATABASE_KEY)
                    .valueAsString(0);
            authDatabase = isBlank(authDatabase) ? "admin"
                    : authDatabase;
        }

        int connectTimeoutMs = getIntProperty(session, catalogAlias, MongoCatalog.CONNECT_TIMEOUT_KEY, DEFAULT_CONNECT_TIMEOUT_MS);
        int socketTimeoutMs = getIntProperty(session, catalogAlias, MongoCatalog.SOCKET_TIMEOUT_KEY, DEFAULT_SOCKET_TIMEOUT_MS);
        int serverSelectionTimeoutMs = getIntProperty(session, catalogAlias, MongoCatalog.SERVER_SELECTION_TIMEOUT_KEY, DEFAULT_SERVER_SELECTION_TIMEOUT_MS);

        String key = cacheKey(connectionString, username, authDatabase, password, connectTimeoutMs, socketTimeoutMs, serverSelectionTimeoutMs);
        final String finalUsername = username;
        final String finalPassword = password;
        final String finalAuthDatabase = authDatabase;
        return clientByKey.compute(key, (k, holder) ->
        {
            if (holder == null)
            {
                return new ClientHolder(createClient(connectionString, finalUsername, finalAuthDatabase, finalPassword, connectTimeoutMs, socketTimeoutMs, serverSelectionTimeoutMs));
            }
            holder.touch();
            return holder;
        }).client;
    }

    static String getConnectionString(IQuerySession session, String catalogAlias)
    {
        String connectionString = session.getCatalogProperty(catalogAlias, MongoCatalog.CONNECTIONSTRING_KEY)
                .valueAsString(0);
        if (isBlank(connectionString))
        {
            throw new IllegalArgumentException("Missing " + MongoCatalog.CONNECTIONSTRING_KEY + " in catalog properties for catalog alias: " + catalogAlias);
        }
        return connectionString;
    }

    /** Reads an integer catalog property without relying on the 3-arg default-value overload (a default interface method that is broken under some JDK/Mockito combinations in tests). */
    private static int getIntProperty(IQuerySession session, String catalogAlias, String key, int defaultValue)
    {
        ValueVector v = session.getCatalogProperty(catalogAlias, key);
        return (v == null
                || v.isNull(0)) ? defaultValue
                        : v.getInt(0);
    }

    /** Builds a cache key from the connection string, (if present) the authentication identity and the effective timeouts. Never includes the raw password. */
    private static String cacheKey(String connectionString, String username, String authDatabase, String password, int connectTimeoutMs, int socketTimeoutMs, int serverSelectionTimeoutMs)
    {
        String credentialPart = isBlank(username) ? ""
                : "|" + username + "|" + authDatabase + "|" + sha256(password);
        return connectionString + credentialPart + "|" + connectTimeoutMs + "|" + socketTimeoutMs + "|" + serverSelectionTimeoutMs;
    }

    private static String sha256(String value)
    {
        try
        {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return Base64.getEncoder()
                    .encodeToString(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        }
        catch (NoSuchAlgorithmException e)
        {
            // SHA-256 is guaranteed to be available on every JDK
            throw new IllegalStateException(e);
        }
    }

    private static MongoClient createClient(String connectionString, String username, String authDatabase, String password, int connectTimeoutMs, int socketTimeoutMs, int serverSelectionTimeoutMs)
    {
        MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToSocketSettings(b -> b.connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                        .readTimeout(socketTimeoutMs, TimeUnit.MILLISECONDS))
                .applyToClusterSettings(b -> b.serverSelectionTimeout(serverSelectionTimeoutMs, TimeUnit.MILLISECONDS));

        if (isNotBlank(username))
        {
            builder.credential(MongoCredential.createCredential(username, authDatabase, password.toCharArray()));
        }

        return MongoClients.create(builder.build());
    }

    /** Closes all cached clients and stops the housekeeping task. */
    void close()
    {
        houseKeepingFuture.cancel(true);
        clientByKey.values()
                .forEach(ClientHolder::close);
        clientByKey.clear();
    }

    private final Runnable houseKeepingRunnable = () ->
    {
        if (clientByKey.isEmpty())
        {
            return;
        }

        Instant threshold = Instant.now()
                .minus(IDLE_MINUTES, ChronoUnit.MINUTES);

        Iterator<Entry<String, ClientHolder>> it = clientByKey.entrySet()
                .iterator();
        while (it.hasNext())
        {
            Entry<String, ClientHolder> entry = it.next();
            if (entry.getValue().lastAccessTime.isBefore(threshold))
            {
                entry.getValue()
                        .close();
                it.remove();
            }
        }
    };

    private static class ClientHolder
    {
        final MongoClient client;
        volatile Instant lastAccessTime;

        ClientHolder(MongoClient client)
        {
            this.client = client;
            this.lastAccessTime = Instant.now();
        }

        void touch()
        {
            lastAccessTime = Instant.now();
        }

        void close()
        {
            try
            {
                client.close();
            }
            catch (Exception e)
            {
                LOGGER.warn("Error closing MongoClient", e);
            }
        }
    }
}
