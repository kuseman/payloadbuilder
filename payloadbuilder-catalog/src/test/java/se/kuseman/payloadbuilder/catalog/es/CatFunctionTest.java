package se.kuseman.payloadbuilder.catalog.es;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/** Test of {@link CatFunction} */
class CatFunctionTest
{
    @Test
    void test_getCatUrl()
    {
        assertEquals("http://localhost:9200/_cat/nodes?format=json", CatFunction.getCatUrl("http://localhost:9200", "nodes"));
        assertEquals("http://localhost:9200/elastic/_cat/nodes?format=json", CatFunction.getCatUrl("http://localhost:9200/elastic", "nodes"));
        assertEquals("http://localhost:9200/elastic/_cat/nodes?s=name&v=&format=json", CatFunction.getCatUrl("http://localhost:9200/elastic", "nodes?s=name&v"));
    }
}
