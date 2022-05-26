package se.kuseman.payloadbuilder.catalog.es;

import org.junit.Assert;
import org.junit.Test;

/** Test of {@link SearchFunction} */
public class SearchFunctionTest extends Assert
{
    @Test(
            expected = IllegalArgumentException.class)
    public void test_getSearchUrl_fail()
    {
        SearchFunction.getSearchTemplateUrl("", "myindex", "_doc", 100, null);
    }

    @Test
    public void test_getSearchTemplateUrl()
    {
        assertEquals("http://localhost:9200/*/_doc/_search/template?filter_path=_scroll_id,hits.hits", SearchFunction.getSearchTemplateUrl("http://localhost:9200", null, "_doc", null, null));
        assertEquals("http://localhost:9200/myIndex/_doc/_search/template?filter_path=_scroll_id,hits.hits",
                SearchFunction.getSearchTemplateUrl("http://localhost:9200", "myIndex", "_doc", null, null));
        assertEquals("http://localhost:9200/myIndex/_doc/_search/template?filter_path=_scroll_id,hits.hits&size=100",
                SearchFunction.getSearchTemplateUrl("http://localhost:9200", "myIndex", "_doc", 100, null));
        assertEquals("http://localhost:9200/myIndex/_doc/_search/template?filter_path=_scroll_id,hits.hits&scroll=2m",
                SearchFunction.getSearchTemplateUrl("http://localhost:9200", "myIndex", "_doc", null, 2));
        assertEquals("http://localhost:9200/myIndex/_doc/_search/template?filter_path=_scroll_id,hits.hits&scroll=2m&size=300",
                SearchFunction.getSearchTemplateUrl("http://localhost:9200", "myIndex", "_doc", 300, 2));
    }
}
