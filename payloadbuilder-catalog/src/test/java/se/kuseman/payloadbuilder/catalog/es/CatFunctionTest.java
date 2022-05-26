package se.kuseman.payloadbuilder.catalog.es;

import org.junit.Assert;
import org.junit.Test;

/** Test of {@link CatFunction} */
public class CatFunctionTest extends Assert
{
    @Test
    public void test_getCatUrl()
    {
        assertEquals("http://localhost:9200/_cat/nodes?format=json", CatFunction.getCatUrl("http://localhost:9200", "nodes"));
        assertEquals("http://localhost:9200/elastic/_cat/nodes?format=json", CatFunction.getCatUrl("http://localhost:9200/elastic", "nodes"));
        assertEquals("http://localhost:9200/elastic/_cat/nodes?s=name&v=&format=json", CatFunction.getCatUrl("http://localhost:9200/elastic", "nodes?s=name&v"));
    }
}
