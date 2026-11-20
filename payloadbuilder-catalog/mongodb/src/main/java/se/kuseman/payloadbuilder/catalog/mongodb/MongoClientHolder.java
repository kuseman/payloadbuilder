package se.kuseman.payloadbuilder.catalog.mongodb;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
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

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.catalog.CredentialsException;

/**
 * Caches one {@link MongoClient} per connection string + authentication identity + timeout configuration + TLS certificate validation flag.
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
 *
 * NOTE! The Java driver's ConnectionString does not support tlsAllowInvalidCertificates (and tlsInsecure only relaxes host name checks), so
 * accepting any server certificate is done through the {@link MongoCatalog#TLS_ALLOW_INVALID_CERTIFICATES_KEY} catalog property, which installs
 * a trust-all SSLContext. It never turns TLS on by itself - that stays the connection string's decision.
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

        boolean allowInvalidCertificates = getBooleanProperty(session, catalogAlias, MongoCatalog.TLS_ALLOW_INVALID_CERTIFICATES_KEY);

        String key = cacheKey(connectionString, username, authDatabase, password, connectTimeoutMs, socketTimeoutMs, serverSelectionTimeoutMs, allowInvalidCertificates);
        final String finalUsername = username;
        final String finalPassword = password;
        final String finalAuthDatabase = authDatabase;
        return clientByKey.compute(key, (k, holder) ->
        {
            if (holder == null)
            {
                return new ClientHolder(MongoClients.create(
                        buildSettings(connectionString, finalUsername, finalAuthDatabase, finalPassword, connectTimeoutMs, socketTimeoutMs, serverSelectionTimeoutMs, allowInvalidCertificates)));
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

    /**
     * Reads a boolean catalog property. A boolean value is used as is, any other value counts as true only if it is the string "true" (ignoring case). Missing/null is false.
     *
     * <pre>
     * NOTE! {@link ValueVector#getBoolean(int)} is not used for strings since it throws on unknown strings and also accepts ie. "yes"/"1".
     * </pre>
     */
    private static boolean getBooleanProperty(IQuerySession session, String catalogAlias, String key)
    {
        ValueVector v = session.getCatalogProperty(catalogAlias, key);
        if (v == null
                || v.isNull(0))
        {
            return false;
        }
        if (v.type()
                .getType() == Type.Boolean)
        {
            return v.getBoolean(0);
        }
        return "true".equalsIgnoreCase(v.valueAsString(0));
    }

    /**
     * Builds a cache key from the connection string, (if present) the authentication identity, the effective timeouts and the TLS certificate validation flag. Never includes the raw password.
     */
    private static String cacheKey(String connectionString, String username, String authDatabase, String password, int connectTimeoutMs, int socketTimeoutMs, int serverSelectionTimeoutMs,
            boolean allowInvalidCertificates)
    {
        String credentialPart = isBlank(username) ? ""
                : "|" + username + "|" + authDatabase + "|" + sha256(password);
        return connectionString + credentialPart + "|" + connectTimeoutMs + "|" + socketTimeoutMs + "|" + serverSelectionTimeoutMs + "|" + allowInvalidCertificates;
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

    /** Builds the client settings. Package private for tests. */
    static MongoClientSettings buildSettings(String connectionString, String username, String authDatabase, String password, int connectTimeoutMs, int socketTimeoutMs, int serverSelectionTimeoutMs,
            boolean allowInvalidCertificates)
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

        if (allowInvalidCertificates)
        {
            // NOTE! Don't call enabled(true) here, whether TLS is on is the connection string's decision
            builder.applyToSslSettings(b -> b.invalidHostNameAllowed(true)
                    .context(trustAllContext()));
        }

        return builder.build();
    }

    /** Return the shared trust-all SSL context. Package private for tests. */
    static SSLContext trustAllContext()
    {
        return TrustAllContextHolder.CONTEXT;
    }

    /** Return the trust manager that {@link #trustAllContext()} is initialised with. Package private for tests. */
    static X509TrustManager trustAllTrustManager()
    {
        return TrustAllContextHolder.TRUST_MANAGER;
    }

    /** Lazily initialised (on first use) trust-all SSL context. Holds no per-connection state so one instance is shared by all clients. */
    private static class TrustAllContextHolder
    {
        static final X509TrustManager TRUST_MANAGER = new X509TrustManager()
        {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType)
            {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType)
            {
            }

            @Override
            public X509Certificate[] getAcceptedIssuers()
            {
                return new X509Certificate[0];
            }
        };
        static final SSLContext CONTEXT = create();

        private static SSLContext create()
        {
            try
            {
                SSLContext context = SSLContext.getInstance("TLS");
                context.init(null, new TrustManager[] { TRUST_MANAGER }, new SecureRandom());
                return context;
            }
            catch (Exception e)
            {
                throw new IllegalStateException("Could not create trust-all SSL context", e);
            }
        }
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
