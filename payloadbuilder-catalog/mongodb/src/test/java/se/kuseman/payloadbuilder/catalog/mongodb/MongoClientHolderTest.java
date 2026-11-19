package se.kuseman.payloadbuilder.catalog.mongodb;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;

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

    private static IQuerySession mockSession(String connectionString, String username, String password, String authDatabase)
    {
        IQuerySession session = mock(IQuerySession.class);
        when(session.getCatalogProperty(CATALOG_ALIAS, MongoCatalog.CONNECTIONSTRING_KEY)).thenReturn(vv(connectionString));
        when(session.getCatalogProperty(CATALOG_ALIAS, MongoCatalog.AUTH_USERNAME_KEY)).thenReturn(vv(username));
        when(session.getCatalogProperty(CATALOG_ALIAS, MongoCatalog.AUTH_PASSWORD_KEY)).thenReturn(vv(password));
        when(session.getCatalogProperty(CATALOG_ALIAS, MongoCatalog.AUTH_DATABASE_KEY)).thenReturn(vv(authDatabase));
        return session;
    }

    private static ValueVector vv(String value)
    {
        return value == null ? ValueVector.literalNull(ResolvedType.ANY, 1)
                : ValueVector.literalString(value, 1);
    }
}
