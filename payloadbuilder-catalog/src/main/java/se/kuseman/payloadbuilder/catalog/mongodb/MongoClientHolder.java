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
import se.kuseman.payloadbuilder.catalog.CredentialsException;

/**
 * Caches one {@link MongoClient} per connection string + authentication identity.
 *
 * <pre>
 * A {@link MongoClient} pools its own connections and is thread safe, but connection strings are often ephemeral
 * (ie. pointing at short lived docker containers) so idle clients are closed and evicted by a housekeeping task,
 * mirroring the JdbcCatalog's pooled datasource housekeeping.
 *
 * NOTE! The cache key includes the username and a hash of the password (not the connection string alone). This matters because
 * a MongoClient/MongoCredential is immutable once built - unlike JdbcCatalog's HikariDataSource, there is no way to swap
 * credentials on an already constructed client. Keying only by connection string would let two different users of the same
 * URI (or a rotated password) silently keep reusing whichever client happened to be built first. Since the key changes when
 * credentials change, a credential change simply results in a new cache entry - the old one is left untouched and ages out
 * via the housekeeping task above.
 * </pre>
 */
class MongoClientHolder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoClientHolder.class);
    private static final long IDLE_MINUTES = 10;

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

        String key = cacheKey(connectionString, username, authDatabase, password);
        final String finalUsername = username;
        final String finalPassword = password;
        final String finalAuthDatabase = authDatabase;
        return clientByKey.compute(key, (k, holder) ->
        {
            if (holder == null)
            {
                return new ClientHolder(createClient(connectionString, finalUsername, finalAuthDatabase, finalPassword));
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

    /** Builds a cache key from the connection string and (if present) the authentication identity. Never includes the raw password. */
    private static String cacheKey(String connectionString, String username, String authDatabase, String password)
    {
        if (isBlank(username))
        {
            return connectionString;
        }
        return connectionString + '|' + username + '|' + authDatabase + '|' + sha256(password);
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

    private static MongoClient createClient(String connectionString, String username, String authDatabase, String password)
    {
        MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString));

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
