package se.kuseman.payloadbuilder.catalog.http;

import java.net.URI;

import org.junit.Assert;
import org.junit.Test;

/** Test of {@link HttpDataSource} */
public class HttpDataSourceTest extends Assert
{
    @Test
    public void test_createUri()
    {
        assertEquals(URI.create("http://localhost"), HttpDataSource.getURI("http://localhost"));
        assertEquals(URI.create("https://localhost:8080"), HttpDataSource.getURI("https://localhost:8080"));
        assertEquals(URI.create("http://user:pass@localhost"), HttpDataSource.getURI("http://user:pass@localhost"));
        assertEquals(URI.create("http://user:pass@localhost/some/path"), HttpDataSource.getURI("http://user:pass@localhost/some/path"));
        assertEquals(URI.create("http://user:pass@localhost/some/path?parm1=123&param2=hello"), HttpDataSource.getURI("http://user:pass@localhost/some/path?parm1=123&param2=hello"));
        assertEquals(URI.create("http://user:pass@localhost/some/path?parm1=r%C3%B6d%20tr%C3%B6ja&param2=hello"),
                HttpDataSource.getURI("http://user:pass@localhost/some/path?parm1=röd tröja&param2=hello"));
        assertEquals(URI.create("http://user:pass@localhost/some/path?parm1=r%C3%B6d%20tr%C3%B6ja&param2=hello#anchor"),
                HttpDataSource.getURI("http://user:pass@localhost/some/path?parm1=röd tröja&param2=hello#anchor"));
    }
}
