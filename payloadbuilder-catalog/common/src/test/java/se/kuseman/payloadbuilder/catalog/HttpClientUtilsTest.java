package se.kuseman.payloadbuilder.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** Test of {@link HttpClientUtils} */
class HttpClientUtilsTest
{
    @Test
    void test_getPassword()
    {
        assertEquals("secret", HttpClientUtils.getPassword("secret"));
        assertEquals("secret", HttpClientUtils.getPassword(new char[] { 's', 'e', 'c', 'r', 'e', 't' }));
        // What a query's "use es.authpassword = 'secret'" stores
        assertEquals("secret", HttpClientUtils.getPassword(UTF8String.from("secret")));
        assertNull(HttpClientUtils.getPassword(null));
        assertNull(HttpClientUtils.getPassword(123));
    }
}
