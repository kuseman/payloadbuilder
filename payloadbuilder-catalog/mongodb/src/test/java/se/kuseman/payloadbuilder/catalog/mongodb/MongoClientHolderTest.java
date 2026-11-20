package se.kuseman.payloadbuilder.catalog.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.cert.X509Certificate;

import javax.net.ssl.X509TrustManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.SslSettings;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/**
 * Unit tests of {@link MongoClientHolder}.
 *
 * <pre>
 * NOTE! MongoClients.create(...) is lazy - it never connects until an operation is executed - so these tests can safely
 * construct clients against a bogus connection string without touching the network or requiring Docker.
 * </pre>
 */
class MongoClientHolderTest
{
    private static final String CATALOG_ALIAS = "mongo";
    private static final String CONNECTION_STRING = "mongodb://localhost:27017";
    private static final String TLS_CONNECTION_STRING = "mongodb://localhost:27017/?tls=true";

    private final MongoClientHolder holder = new MongoClientHolder();

    @AfterEach
    void teardown()
    {
        holder.close();
    }

    @Test
    void test_same_connection_string_and_credentials_reuse_the_same_client()
    {
        IQuerySession session = mockSession(CONNECTION_STRING, "user", "pass", null);

        MongoClient first = holder.getClient(session, CATALOG_ALIAS);
        MongoClient second = holder.getClient(session, CATALOG_ALIAS);

        assertSame(first, second);
    }

    @Test
    void test_different_username_on_same_connection_string_gets_a_different_client()
    {
        IQuerySession sessionA = mockSession(CONNECTION_STRING, "userA", "pass", null);
        IQuerySession sessionB = mockSession(CONNECTION_STRING, "userB", "pass", null);

        MongoClient clientA = holder.getClient(sessionA, CATALOG_ALIAS);
        MongoClient clientB = holder.getClient(sessionB, CATALOG_ALIAS);

        assertNotSame(clientA, clientB);
    }

    @Test
    void test_different_password_on_same_connection_string_and_username_gets_a_different_client()
    {
        // ie. a rotated/changed password
        IQuerySession sessionA = mockSession(CONNECTION_STRING, "user", "oldpass", null);
        IQuerySession sessionB = mockSession(CONNECTION_STRING, "user", "newpass", null);

        MongoClient clientA = holder.getClient(sessionA, CATALOG_ALIAS);
        MongoClient clientB = holder.getClient(sessionB, CATALOG_ALIAS);

        assertNotSame(clientA, clientB);
    }

    @Test
    void test_different_auth_database_on_same_connection_string_and_credentials_gets_a_different_client()
    {
        IQuerySession sessionA = mockSession(CONNECTION_STRING, "user", "pass", "dbA");
        IQuerySession sessionB = mockSession(CONNECTION_STRING, "user", "pass", "dbB");

        MongoClient clientA = holder.getClient(sessionA, CATALOG_ALIAS);
        MongoClient clientB = holder.getClient(sessionB, CATALOG_ALIAS);

        assertNotSame(clientA, clientB);
    }

    @Test
    void test_no_auth_connections_with_same_connection_string_reuse_the_same_client()
    {
        IQuerySession sessionA = mockSession(CONNECTION_STRING, null, null, null);
        IQuerySession sessionB = mockSession(CONNECTION_STRING, null, null, null);

        MongoClient clientA = holder.getClient(sessionA, CATALOG_ALIAS);
        MongoClient clientB = holder.getClient(sessionB, CATALOG_ALIAS);

        assertSame(clientA, clientB);
    }

    @Test
    void test_settings_without_allow_invalid_certificates_keep_driver_defaults()
    {
        MongoClientSettings settings = MongoClientHolder.buildSettings(TLS_CONNECTION_STRING, null, null, null, 1000, 1000, 1000, false);
        SslSettings ssl = settings.getSslSettings();

        assertTrue(ssl.isEnabled());
        assertNull(ssl.getContext());
        assertFalse(ssl.isInvalidHostNameAllowed());
    }

    @Test
    void test_settings_with_allow_invalid_certificates_trust_any_certificate() throws Exception
    {
        MongoClientSettings settings = MongoClientHolder.buildSettings(TLS_CONNECTION_STRING, null, null, null, 1000, 1000, 1000, true);
        SslSettings ssl = settings.getSslSettings();

        assertTrue(ssl.isEnabled());
        assertTrue(ssl.isInvalidHostNameAllowed());
        assertSame(MongoClientHolder.trustAllContext(), ssl.getContext());
        assertNotNull(ssl.getContext());

        // NOTE! SSLContext doesn't expose its trust managers so the one the context is initialised with is verified directly
        X509TrustManager trustManager = MongoClientHolder.trustAllTrustManager();
        // Any chain (ie. a self signed one) must be accepted
        X509Certificate selfSigned = mock(X509Certificate.class);
        trustManager.checkServerTrusted(new X509Certificate[] { selfSigned }, "RSA");
        trustManager.checkServerTrusted(new X509Certificate[0], "RSA");
        assertEquals(0, trustManager.getAcceptedIssuers().length);
    }

    @Test
    void test_allow_invalid_certificates_does_not_turn_on_tls()
    {
        MongoClientSettings settings = MongoClientHolder.buildSettings(CONNECTION_STRING, null, null, null, 1000, 1000, 1000, true);
        assertFalse(settings.getSslSettings()
                .isEnabled());
    }

    @Test
    void test_allow_invalid_certificates_is_part_of_cache_key()
    {
        MongoClient absent = holder.getClient(mockSession(TLS_CONNECTION_STRING, null, null, null), CATALOG_ALIAS);
        MongoClient nullValue = holder.getClient(mockSession(TLS_CONNECTION_STRING, null, null, null, ValueVector.literalNull(ResolvedType.BOOLEAN, 1)), CATALOG_ALIAS);
        MongoClient falseValue = holder.getClient(mockSession(TLS_CONNECTION_STRING, null, null, null, ValueVector.literalBoolean(false, 1)), CATALOG_ALIAS);
        MongoClient otherString = holder.getClient(mockSession(TLS_CONNECTION_STRING, null, null, null, vv("yes")), CATALOG_ALIAS);
        // NOTE! Deliberately stricter than ValueVector#getBoolean (which treats "1"/1 as true): only true and "true" opt in to
        // disabling certificate validation. Don't relax this to match getBoolean.
        MongoClient oneString = holder.getClient(mockSession(TLS_CONNECTION_STRING, null, null, null, vv("1")), CATALOG_ALIAS);
        MongoClient oneInt = holder.getClient(mockSession(TLS_CONNECTION_STRING, null, null, null, ValueVector.literalInt(1, 1)), CATALOG_ALIAS);
        MongoClient trueValue = holder.getClient(mockSession(TLS_CONNECTION_STRING, null, null, null, ValueVector.literalBoolean(true, 1)), CATALOG_ALIAS);
        MongoClient trueValueAgain = holder.getClient(mockSession(TLS_CONNECTION_STRING, null, null, null, ValueVector.literalBoolean(true, 1)), CATALOG_ALIAS);
        MongoClient trueString = holder.getClient(mockSession(TLS_CONNECTION_STRING, null, null, null, vv("TRUE")), CATALOG_ALIAS);

        // Absent, null, false and anything but "true" are all false
        assertSame(absent, nullValue);
        assertSame(absent, falseValue);
        assertSame(absent, otherString);
        assertSame(absent, oneString);
        assertSame(absent, oneInt);

        assertNotSame(absent, trueValue);
        assertSame(trueValue, trueValueAgain);
        // The string "true" counts as true
        assertSame(trueValue, trueString);
    }

    private static IQuerySession mockSession(String connectionString, String username, String password, String authDatabase)
    {
        return mockSession(connectionString, username, password, authDatabase, null);
    }

    private static IQuerySession mockSession(String connectionString, String username, String password, String authDatabase, ValueVector allowInvalidCertificates)
    {
        IQuerySession session = mock(IQuerySession.class);
        when(session.getCatalogProperty(CATALOG_ALIAS, MongoCatalog.CONNECTIONSTRING_KEY)).thenReturn(vv(connectionString));
        when(session.getCatalogProperty(CATALOG_ALIAS, MongoCatalog.AUTH_USERNAME_KEY)).thenReturn(vv(username));
        when(session.getCatalogProperty(CATALOG_ALIAS, MongoCatalog.AUTH_PASSWORD_KEY)).thenReturn(vv(password));
        when(session.getCatalogProperty(CATALOG_ALIAS, MongoCatalog.AUTH_DATABASE_KEY)).thenReturn(vv(authDatabase));
        when(session.getCatalogProperty(CATALOG_ALIAS, MongoCatalog.TLS_ALLOW_INVALID_CERTIFICATES_KEY)).thenReturn(allowInvalidCertificates);
        return session;
    }

    private static ValueVector vv(String value)
    {
        return value == null ? ValueVector.literalNull(ResolvedType.ANY, 1)
                : ValueVector.literalString(value, 1);
    }
}
