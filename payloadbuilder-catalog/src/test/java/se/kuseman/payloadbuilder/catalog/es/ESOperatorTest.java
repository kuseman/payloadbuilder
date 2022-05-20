package se.kuseman.payloadbuilder.catalog.es;

import org.junit.Assert;
import org.junit.Test;

/** Test of {@link ESOperator} */
public class ESOperatorTest extends Assert
{
    @Test
    public void test_getSearchUrl()
    {
        assertEquals("http://localhost:9200/*/_doc/_search?filter_path=_scroll_id,hits.hits", ESOperator.getSearchUrl("http://localhost:9200", null, "_doc", null, null, null));
        assertEquals("http://localhost:9200/myindex/_search?filter_path=_scroll_id,hits.hits", ESOperator.getSearchUrl("http://localhost:9200", "myindex", null, null, null, null));
        assertEquals("http://localhost:9200/myindex/_doc/_search?filter_path=_scroll_id,hits.hits", ESOperator.getSearchUrl("http://localhost:9200", "myindex", "_doc", null, null, null));
        assertEquals("http://localhost:9200/myindex/_doc/_search?filter_path=_scroll_id,hits.hits&size=100", ESOperator.getSearchUrl("http://localhost:9200", "myindex", "_doc", 100, null, null));
        assertEquals("http://localhost:9200/myindex/_doc/_search?filter_path=_scroll_id,hits.hits&scroll=2m", ESOperator.getSearchUrl("http://localhost:9200", "myindex", "_doc", null, 2, null));
        assertEquals("http://localhost:9200/myindex/_doc/_search?filter_path=_scroll_id,hits.hits&scroll=2m&size=200",
                ESOperator.getSearchUrl("http://localhost:9200", "myindex", "_doc", 200, 2, null));
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_getSearchUrl_fail()
    {
        ESOperator.getSearchUrl("", "myindex", "_doc", 100, null, null);
    }

    @Test
    public void test_getScrollUrl()
    {
        assertEquals("http://localhost:9200/_search/scroll?scroll=2m&filter_path=_scroll_id,hits.hits", ESOperator.getScrollUrl("http://localhost:9200", 2));
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_getScrollUrl_fail()
    {
        ESOperator.getScrollUrl("", 2);
    }

    @Test
    public void test_getMgetUrl()
    {
        assertEquals("http://localhost:9200/myindex/type/_mget", ESOperator.getMgetUrl("http://localhost:9200", "myindex", "type"));
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_getMgetUrl_fail()
    {
        ESOperator.getMgetUrl("", "", "");
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_getMgetUrl_fail_1()
    {
        ESOperator.getMgetUrl("index", "", "");
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_getMgetUrl_fail_2()
    {
        ESOperator.getMgetUrl("index", "type", "");
    }
}
