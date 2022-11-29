package se.kuseman.payloadbuilder.catalog.es;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static se.kuseman.payloadbuilder.test.IOrdinalValuesMock.ordinalValues;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import se.kuseman.payloadbuilder.api.catalog.IAnalyzePair;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.IIndexPredicate;
import se.kuseman.payloadbuilder.api.operator.IOrdinalValues;
import se.kuseman.payloadbuilder.catalog.es.ESCatalog.MappedProperty;
import se.kuseman.payloadbuilder.catalog.es.ESUtils.SortItemMeta;
import se.kuseman.payloadbuilder.test.IAnalyzePairMock;

/** Test of {@link ESUtils} */
public class ESUtilsTest extends Assert
{
    private IExecutionContext context = Mockito.mock(IExecutionContext.class);

    @Test(
            expected = IllegalArgumentException.class)
    public void test_getSearchTemplateUrl_fail()
    {
        ESUtils.getSearchTemplateUrl(true, "", "myindex", "_doc", 100, null);
    }

    @Test
    public void test_getSearchTemplateUrl()
    {
        assertEquals("http://localhost:9200/*/_doc/_search/template?filter_path=_scroll_id,hits.hits", ESUtils.getSearchTemplateUrl(true, "http://localhost:9200", null, "_doc", null, null));
        assertEquals("http://localhost:9200/myIndex/_doc/_search/template?filter_path=_scroll_id,hits.hits",
                ESUtils.getSearchTemplateUrl(true, "http://localhost:9200", "myIndex", "_doc", null, null));
        assertEquals("http://localhost:9200/myIndex/_doc/_search/template?filter_path=_scroll_id,hits.hits&size=100",
                ESUtils.getSearchTemplateUrl(true, "http://localhost:9200", "myIndex", "_doc", 100, null));
        assertEquals("http://localhost:9200/myIndex/_doc/_search/template?filter_path=_scroll_id,hits.hits&scroll=2m",
                ESUtils.getSearchTemplateUrl(true, "http://localhost:9200", "myIndex", "_doc", null, 2));
        assertEquals("http://localhost:9200/myIndex/_doc/_search/template?filter_path=_scroll_id,hits.hits&scroll=2m&size=300",
                ESUtils.getSearchTemplateUrl(true, "http://localhost:9200", "myIndex", "_doc", 300, 2));

        // No use of _doc type
        assertEquals("http://localhost:9200/*/_search/template?filter_path=_scroll_id,hits.hits", ESUtils.getSearchTemplateUrl(false, "http://localhost:9200", null, "_doc", null, null));
        assertEquals("http://localhost:9200/myIndex/_search/template?filter_path=_scroll_id,hits.hits", ESUtils.getSearchTemplateUrl(false, "http://localhost:9200", "myIndex", "_doc", null, null));
        assertEquals("http://localhost:9200/myIndex/_search/template?filter_path=_scroll_id,hits.hits&size=100",
                ESUtils.getSearchTemplateUrl(false, "http://localhost:9200", "myIndex", "_doc", 100, null));
        assertEquals("http://localhost:9200/myIndex/_search/template?filter_path=_scroll_id,hits.hits&scroll=2m",
                ESUtils.getSearchTemplateUrl(false, "http://localhost:9200", "myIndex", "_doc", null, 2));
        assertEquals("http://localhost:9200/myIndex/_search/template?filter_path=_scroll_id,hits.hits&scroll=2m&size=300",
                ESUtils.getSearchTemplateUrl(false, "http://localhost:9200", "myIndex", "_doc", 300, 2));
    }

    @Test
    public void test_getSearchUrl()
    {
        assertEquals("http://localhost:9200/*/_doc/_search?filter_path=_scroll_id,hits.hits", ESUtils.getSearchUrl(true, "http://localhost:9200", null, "_doc", null, null, null));
        assertEquals("http://localhost:9200/myindex/_search?filter_path=_scroll_id,hits.hits", ESUtils.getSearchUrl(true, "http://localhost:9200", "myindex", null, null, null, null));
        assertEquals("http://localhost:9200/myindex/_doc/_search?filter_path=_scroll_id,hits.hits", ESUtils.getSearchUrl(true, "http://localhost:9200", "myindex", "_doc", null, null, null));
        assertEquals("http://localhost:9200/myindex/_doc/_search?filter_path=_scroll_id,hits.hits&size=100", ESUtils.getSearchUrl(true, "http://localhost:9200", "myindex", "_doc", 100, null, null));
        assertEquals("http://localhost:9200/myindex/_doc/_search?filter_path=_scroll_id,hits.hits&scroll=2m", ESUtils.getSearchUrl(true, "http://localhost:9200", "myindex", "_doc", null, 2, null));
        assertEquals("http://localhost:9200/myindex/_doc/_search?filter_path=_scroll_id,hits.hits&scroll=2m&size=200",
                ESUtils.getSearchUrl(true, "http://localhost:9200", "myindex", "_doc", 200, 2, null));

        // No use of _doc type
        assertEquals("http://localhost:9200/*/_search?filter_path=_scroll_id,hits.hits", ESUtils.getSearchUrl(false, "http://localhost:9200", null, "_doc", null, null, null));
        assertEquals("http://localhost:9200/myindex/_search?filter_path=_scroll_id,hits.hits", ESUtils.getSearchUrl(false, "http://localhost:9200", "myindex", null, null, null, null));
        assertEquals("http://localhost:9200/myindex/_search?filter_path=_scroll_id,hits.hits", ESUtils.getSearchUrl(false, "http://localhost:9200", "myindex", "_doc", null, null, null));
        assertEquals("http://localhost:9200/myindex/_search?filter_path=_scroll_id,hits.hits&size=100", ESUtils.getSearchUrl(false, "http://localhost:9200", "myindex", "_doc", 100, null, null));
        assertEquals("http://localhost:9200/myindex/_search?filter_path=_scroll_id,hits.hits&scroll=2m", ESUtils.getSearchUrl(false, "http://localhost:9200", "myindex", "_doc", null, 2, null));
        assertEquals("http://localhost:9200/myindex/_search?filter_path=_scroll_id,hits.hits&scroll=2m&size=200",
                ESUtils.getSearchUrl(false, "http://localhost:9200", "myindex", "_doc", 200, 2, null));
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_getSearchUrl_fail()
    {
        ESUtils.getSearchUrl(true, "", "myindex", "_doc", 100, null, null);
    }

    @Test
    public void test_getScrollUrl()
    {
        assertEquals("http://localhost:9200/_search/scroll?scroll=2m&filter_path=_scroll_id,hits.hits", ESUtils.getScrollUrl("http://localhost:9200", 2));
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_getScrollUrl_fail()
    {
        ESUtils.getScrollUrl("", 2);
    }

    @Test
    public void test_getMgetUrl()
    {
        assertEquals("http://localhost:9200/myindex/type/_mget", ESUtils.getMgetUrl(true, "http://localhost:9200", "myindex", "type"));
        assertEquals("http://localhost:9200/myindex/type/_mget", ESUtils.getMgetUrl(false, "http://localhost:9200", "myindex", "type"));
        assertEquals("http://localhost:9200/myindex/_doc/_mget", ESUtils.getMgetUrl(true, "http://localhost:9200", "myindex", "_doc"));
        assertEquals("http://localhost:9200/myindex/_mget", ESUtils.getMgetUrl(false, "http://localhost:9200", "myindex", "_doc"));
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_getMgetUrl_fail()
    {
        ESUtils.getMgetUrl(true, "", "", "");
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_getMgetUrl_fail_1()
    {
        ESUtils.getMgetUrl(true, "index", "", "");
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_getMgetUrl_fail_2()
    {
        ESUtils.getMgetUrl(true, "index", "type", "");
    }

    @Test
    public void test_search_body_singleType_with_index()
    {
        // field1 != 20
        List<IAnalyzePair> pairs = asList(IAnalyzePairMock.neq("field1", 20));

        // Null outer values should yield a dummy value
        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"terms\":{\"index.keyword\":[\"<index values>\"]}}],\"must_not\":[{\"term\":{\"field1\":20}}]}}}",
                ESUtils.getSearchBody(emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false))
                        .collect(toList()), null, "index.keyword", true, true, context));

        IIndexPredicate indexPredicate = Mockito.mock(IIndexPredicate.class);
        List<IOrdinalValues> outerValues = asList();
        when(indexPredicate.getOuterValuesIterator(any())).thenReturn(outerValues.iterator());

        // Empty outer values should not yield any
        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"must_not\":[{\"term\":{\"field1\":20}}]}}}", ESUtils.getSearchBody(emptyList(), pairs.stream()
                .map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false))
                .collect(toList()), indexPredicate, "index.keyword", false, true, context));

        outerValues = asList(ordinalValues(1), ordinalValues(2), ordinalValues(null));
        when(indexPredicate.getOuterValuesIterator(any())).thenReturn(outerValues.iterator());

        // Non quote
        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"terms\":{\"index.keyword\":[1,2]}}],\"must_not\":[{\"term\":{\"field1\":20}}]}}}",
                ESUtils.getSearchBody(emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false))
                        .collect(toList()), indexPredicate, "index.keyword", false, true, context));

        // Quote
        when(indexPredicate.getOuterValuesIterator(any())).thenReturn(outerValues.iterator());
        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"terms\":{\"index.keyword\":[\"1\",\"2\"]}}],\"must_not\":[{\"term\":{\"field1\":20}}]}}}",
                ESUtils.getSearchBody(emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false))
                        .collect(toList()), indexPredicate, "index.keyword", true, true, context));
    }

    @Test
    public void test_search_body_singleType()
    {
        assertEquals("{\"sort\":[\"_doc\"]}", ESUtils.getSearchBody(emptyList(), emptyList(), null, null, false, true, context));
        assertEquals(
                // CSOFF
                "{\"sort\":[{\"@timestamp\":{\"order\":\"desc\",\"missing\":\"_last\"}},{\"count\":{\"order\":\"asc\"}},{\"nested.object.value\":{\"order\":\"asc\",\"nested\":{\"path\":\"nested.object\"}}},{\"text.keyword\":{\"order\":\"asc\"}}]}",
                // CSON
                ESUtils.getSearchBody(asList(
                        // Null order
                        sortItem("@timestamp", "int", null, Order.DESC, NullOrder.LAST),
                        // No null order
                        sortItem("count", "text", null, Order.ASC, NullOrder.UNDEFINED),
                        // Nested
                        sortItem("nested.object.value", "int", "nested.object", Order.ASC, NullOrder.UNDEFINED),
                        // Free-text property with a non free text field, then we should sort on the non-free-text variant
                        sortItem("text", "text", null, Order.ASC, NullOrder.UNDEFINED, prop("text.keyword", "keyword", null))), emptyList(), null, null, false, true, context));

        List<IAnalyzePair> pairs = getMockPairs();
        assertEquals(
                // CSOFF
                "{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"wildcard\":{\"field4\":{\"value\":\"other*string?end\"}}},{\"wildcard\":{\"field3\":{\"value\":\"some*string?end\"}}},{\"terms\":{\"field\":[1,2,3]}},{\"term\":{\"count\":10}},{\"range\":{\"timestamp\":{\"lte\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"gte\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"lt\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"gt\":\"20201010T10:10:10:000Z\"}}}],\"must_not\":[{\"terms\":{\"field7\":[4,5,6]}},{\"wildcard\":{\"field6\":{\"value\":\"other?string*end\"}}},{\"wildcard\":{\"field5\":{\"value\":\"some?string*end\"}}},{\"term\":{\"field2\":true}},{\"term\":{\"field1\":20}}]}}}",
                // CSON
                ESUtils.getSearchBody(emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false))
                        .collect(toList()), null, null, false, true, context));

        pairs = asList(IAnalyzePairMock.neq("field1", 20));
        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"must_not\":[{\"term\":{\"field1\":20}}]}}}", ESUtils.getSearchBody(emptyList(), pairs.stream()
                .map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false))
                .collect(toList()), null, null, false, true, context));

        pairs = asList(IAnalyzePairMock.notLike("field5", "some_string%end"));
        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"must_not\":[{\"wildcard\":{\"field5\":{\"value\":\"some?string*end\"}}}]}}}", ESUtils.getSearchBody(emptyList(), pairs.stream()
                .map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false))
                .collect(toList()), null, null, false, true, context));

        pairs = asList(IAnalyzePairMock.like("field4", "other%string_end"));
        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"wildcard\":{\"field4\":{\"value\":\"other*string?end\"}}}]}}}", ESUtils.getSearchBody(emptyList(), pairs.stream()
                .map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false))
                .collect(toList()), null, null, false, true, context));

        ScalarFunctionInfo functionInfo = new MatchFunction(new ESCatalog());
        pairs = asList(IAnalyzePairMock.function(functionInfo, asList("field4", "some phrase")));
        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"match\":{\"field4\":\"some phrase\"}}]}}}", ESUtils.getSearchBody(emptyList(), pairs.stream()
                .map(p -> new PropertyPredicate("t", "", p, true))
                .collect(toList()), null, null, false, true, context));

        functionInfo = new QueryFunction(new ESCatalog());
        pairs = asList(IAnalyzePairMock.function(functionInfo, asList("field4:'some phrase'")));
        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"query_string\":{\"query\":\"field4:'some phrase'\"}}]}}}", ESUtils.getSearchBody(emptyList(), pairs.stream()
                .map(p -> new PropertyPredicate("t", "", p, true))
                .collect(toList()), null, null, false, true, context));
    }

    @Test
    public void test_search_body_with_index()
    {
        IIndexPredicate indexPredicate = Mockito.mock(IIndexPredicate.class);
        List<IOrdinalValues> outerValues = asList();
        when(indexPredicate.getOuterValuesIterator(any())).thenReturn(outerValues.iterator());

        // field1 != 20
        List<IAnalyzePair> pairs = asList(IAnalyzePairMock.neq("field1", 20));

        // Empty outer values should not yield any
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must_not\":[{\"term\":{\"field1\":20}}]}}}", ESUtils.getSearchBody(emptyList(), pairs.stream()
                .map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false))
                .collect(toList()), indexPredicate, "index.keyword", false, false, context));

        outerValues = asList(ordinalValues(1), ordinalValues(2));
        when(indexPredicate.getOuterValuesIterator(any())).thenReturn(outerValues.iterator());

        // Non quote
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"terms\":{\"index.keyword\":[1,2]}}],\"must_not\":[{\"term\":{\"field1\":20}}]}}}",
                ESUtils.getSearchBody(emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false))
                        .collect(toList()), indexPredicate, "index.keyword", false, false, context));

        // Quote
        when(indexPredicate.getOuterValuesIterator(any())).thenReturn(outerValues.iterator());
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"terms\":{\"index.keyword\":[\"1\",\"2\"]}}],\"must_not\":[{\"term\":{\"field1\":20}}]}}}",
                ESUtils.getSearchBody(emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false))
                        .collect(toList()), indexPredicate, "index.keyword", true, false, context));
    }

    @Test
    public void test_search_body()
    {
        assertEquals("{\"sort\":[\"_doc\"]}", ESUtils.getSearchBody(emptyList(), emptyList(), null, null, false, false, context));
        assertEquals(
                // CSOFF
                "{\"sort\":[{\"@timestamp\":{\"order\":\"desc\",\"missing\":\"_first\"}},{\"count\":{\"order\":\"asc\"}},{\"nested.object.value\":{\"order\":\"asc\",\"nested_path\":\"nested.object\"}},{\"text.raw\":{\"order\":\"asc\"}}]}",
                // CSON
                ESUtils.getSearchBody(asList(
                        // Null order
                        sortItem("@timestamp", "int", null, Order.DESC, NullOrder.FIRST),
                        // No null order
                        sortItem("count", "string", null, Order.ASC, NullOrder.UNDEFINED),
                        // Nested
                        sortItem("nested.object.value", "int", "nested.object", Order.ASC, NullOrder.UNDEFINED),
                        // Free-text property with a non free text field, then we should sort on the non-free-text variant
                        sortItem("text", "string", null, Order.ASC, NullOrder.UNDEFINED, prop("text.raw", "string", "not_analyzed"))), emptyList(), null, null, false, false, context));

        List<IAnalyzePair> pairs = getMockPairs();
        assertEquals(
                // CSOFF
                "{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"wildcard\":{\"field4\":{\"value\":\"other*string?end\"}}},{\"wildcard\":{\"field3\":{\"value\":\"some*string?end\"}}},{\"terms\":{\"field\":[1,2,3]}},{\"term\":{\"count\":10}},{\"range\":{\"timestamp\":{\"lte\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"gte\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"lt\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"gt\":\"20201010T10:10:10:000Z\"}}}],\"must_not\":[{\"terms\":{\"field7\":[4,5,6]}},{\"wildcard\":{\"field6\":{\"value\":\"other?string*end\"}}},{\"wildcard\":{\"field5\":{\"value\":\"some?string*end\"}}},{\"term\":{\"field2\":true}},{\"term\":{\"field1\":20}}]}}}",
                // CSON
                ESUtils.getSearchBody(emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false))
                        .collect(toList()), null, null, false, false, context));

        pairs = asList(IAnalyzePairMock.neq("field1", 20));
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must_not\":[{\"term\":{\"field1\":20}}]}}}", ESUtils.getSearchBody(emptyList(), pairs.stream()
                .map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false))
                .collect(toList()), null, null, false, false, context));

        pairs = asList(IAnalyzePairMock.eq("field1", 20));
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"term\":{\"field1\":20}}]}}}", ESUtils.getSearchBody(emptyList(), pairs.stream()
                .map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false))
                .collect(toList()), null, null, false, false, context));

        pairs = asList(IAnalyzePairMock.notLike("field5", "some_string%end"));
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must_not\":[{\"wildcard\":{\"field5\":{\"value\":\"some?string*end\"}}}]}}}", ESUtils.getSearchBody(emptyList(), pairs.stream()
                .map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false))
                .collect(toList()), null, null, false, false, context));

        pairs = asList(IAnalyzePairMock.like("field4", "other%string_end"));
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"wildcard\":{\"field4\":{\"value\":\"other*string?end\"}}}]}}}", ESUtils.getSearchBody(emptyList(), pairs.stream()
                .map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false))
                .collect(toList()), null, null, false, false, context));

        ScalarFunctionInfo functionInfo = new MatchFunction(new ESCatalog());
        pairs = asList(IAnalyzePairMock.function(functionInfo, asList("field4", "some phrase")));
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"match\":{\"field4\":\"some phrase\"}}]}}}", ESUtils.getSearchBody(emptyList(), pairs.stream()
                .map(p -> new PropertyPredicate("t", "", p, true))
                .collect(toList()), null, null, false, false, context));

        pairs = asList(IAnalyzePairMock.function(functionInfo, asList("field4,field5", "some phrase")));
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"multi_match\":{\"fields\":[\"field4\",\"field5\"],\"query\":\"some phrase\"}}]}}}",
                ESUtils.getSearchBody(emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate("t", "", p, true))
                        .collect(toList()), null, null, false, false, context));

        functionInfo = new QueryFunction(new ESCatalog());
        pairs = asList(IAnalyzePairMock.function(functionInfo, asList("field4:'some phrase'")));
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"query_string\":{\"query\":\"field4:'some phrase'\"}}]}}}", ESUtils.getSearchBody(emptyList(), pairs.stream()
                .map(p -> new PropertyPredicate("t", "", p, true))
                .collect(toList()), null, null, false, false, context));
    }

    private List<IAnalyzePair> getMockPairs()
    {
        // @formatter:off
        List<IAnalyzePair> pairs = asList(
                IAnalyzePairMock.like("field4", "other%string_end"),
                IAnalyzePairMock.like("field3", "some%string_end"),
                IAnalyzePairMock.in("field", asList(1,2,3)),
                IAnalyzePairMock.eq("count", 10),
                IAnalyzePairMock.lte("timestamp", "20201010T10:10:10:000Z"),
                IAnalyzePairMock.gte("timestamp", "20201010T10:10:10:000Z"),
                IAnalyzePairMock.lt("timestamp", "20201010T10:10:10:000Z"),
                IAnalyzePairMock.gt("timestamp", "20201010T10:10:10:000Z"),
                IAnalyzePairMock.notIn("field7", asList(4,5,6)),
                IAnalyzePairMock.notLike("field6", "other_string%end"),
                IAnalyzePairMock.notLike("field5", "some_string%end"),
                IAnalyzePairMock.neq("field2", true),
                IAnalyzePairMock.neq("field1", 20)
                );
        // @formatter:on
        return pairs;
    }

    private SortItemMeta sortItem(String name, String type, String nestedPath, Order order, NullOrder nullOrder)
    {
        return sortItem(name, type, nestedPath, order, nullOrder, null);
    }

    private SortItemMeta sortItem(String name, String type, String nestedPath, Order order, NullOrder nullOrder, ESCatalog.MappedProperty field)
    {
        ESCatalog.MappedProperty prop = new ESCatalog.MappedProperty(name, type, null, nestedPath, field != null ? singletonList(field)
                : emptyList(), emptyMap());
        return new SortItemMeta(prop, order, nullOrder);
    }

    static MappedProperty prop(String name, String type, Object index)
    {
        return new MappedProperty(name, type, index, null, emptyList(), emptyMap());
    }
}
