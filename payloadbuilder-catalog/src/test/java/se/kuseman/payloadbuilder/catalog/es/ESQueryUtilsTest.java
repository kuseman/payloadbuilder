package se.kuseman.payloadbuilder.catalog.es;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IColumnExpression;
import se.kuseman.payloadbuilder.catalog.es.ESQueryUtils.SortItemMeta;
import se.kuseman.payloadbuilder.catalog.es.ElasticsearchMetaUtils.MappedProperty;
import se.kuseman.payloadbuilder.test.IPredicateMock;

/** Test of {@link ESQueryUtils} */
public class ESQueryUtilsTest extends Assert
{
    private IExecutionContext context = Mockito.mock(IExecutionContext.class);

    @Test(
            expected = IllegalArgumentException.class)
    public void test_getSearchTemplateUrl_fail()
    {
        ESQueryUtils.getSearchUrl("", "myindex", "_doc", 100, null, true);
    }

    @Test
    public void test_getSearchTemplateUrl()
    {
        // Global type (_doc)
        assertEquals("http://localhost:9200/*/_search/template?filter_path=_scroll_id,hits.hits&ignore_unavailable=true",
                ESQueryUtils.getSearchUrl("http://localhost:9200", null, "_doc", null, null, true));
        assertEquals("http://localhost:9200/myIndex/_search/template?filter_path=_scroll_id,hits.hits&ignore_unavailable=true",
                ESQueryUtils.getSearchUrl("http://localhost:9200", "myIndex", "_doc", null, null, true));
        assertEquals("http://localhost:9200/myIndex/_search/template?filter_path=_scroll_id,hits.hits&ignore_unavailable=true&size=100",
                ESQueryUtils.getSearchUrl("http://localhost:9200", "myIndex", "_doc", 100, null, true));
        assertEquals("http://localhost:9200/myIndex/_search/template?filter_path=_scroll_id,hits.hits&ignore_unavailable=true&scroll=2m",
                ESQueryUtils.getSearchUrl("http://localhost:9200", "myIndex", "_doc", null, 2, true));
        assertEquals("http://localhost:9200/myIndex/_search/template?filter_path=_scroll_id,hits.hits&ignore_unavailable=true&scroll=2m&size=300",
                ESQueryUtils.getSearchUrl("http://localhost:9200", "myIndex", "_doc", 300, 2, true));

        // With specific type
        assertEquals("http://localhost:9200/*/type/_search/template?filter_path=_scroll_id,hits.hits&ignore_unavailable=true",
                ESQueryUtils.getSearchUrl("http://localhost:9200", null, "type", null, null, true));
        assertEquals("http://localhost:9200/myIndex/type/_search/template?filter_path=_scroll_id,hits.hits&ignore_unavailable=true",
                ESQueryUtils.getSearchUrl("http://localhost:9200", "myIndex", "type", null, null, true));
        assertEquals("http://localhost:9200/myIndex/type/_search/template?filter_path=_scroll_id,hits.hits&ignore_unavailable=true&size=100",
                ESQueryUtils.getSearchUrl("http://localhost:9200", "myIndex", "type", 100, null, true));
        assertEquals("http://localhost:9200/myIndex/type/_search/template?filter_path=_scroll_id,hits.hits&ignore_unavailable=true&scroll=2m",
                ESQueryUtils.getSearchUrl("http://localhost:9200", "myIndex", "type", null, 2, true));
        assertEquals("http://localhost:9200/myIndex/type/_search/template?filter_path=_scroll_id,hits.hits&ignore_unavailable=true&scroll=2m&size=300",
                ESQueryUtils.getSearchUrl("http://localhost:9200", "myIndex", "type", 300, 2, true));

    }

    @Test
    public void test_getSearchUrl()
    {
        assertEquals("http://localhost:9200/*/_search?filter_path=_scroll_id,hits.hits&ignore_unavailable=true", ESQueryUtils.getSearchUrl("http://localhost:9200", null, "_doc", null, null, false));
        assertEquals("http://localhost:9200/myindex/_search?filter_path=_scroll_id,hits.hits&ignore_unavailable=true",
                ESQueryUtils.getSearchUrl("http://localhost:9200", "myindex", null, null, null, false));
        assertEquals("http://localhost:9200/myindex/_search?filter_path=_scroll_id,hits.hits&ignore_unavailable=true",
                ESQueryUtils.getSearchUrl("http://localhost:9200", "myindex", "_doc", null, null, false));
        assertEquals("http://localhost:9200/myindex/_search?filter_path=_scroll_id,hits.hits&ignore_unavailable=true&size=100",
                ESQueryUtils.getSearchUrl("http://localhost:9200", "myindex", "_doc", 100, null, false));
        assertEquals("http://localhost:9200/myindex/_search?filter_path=_scroll_id,hits.hits&ignore_unavailable=true&scroll=2m",
                ESQueryUtils.getSearchUrl("http://localhost:9200", "myindex", "_doc", null, 2, false));
        assertEquals("http://localhost:9200/myindex/_search?filter_path=_scroll_id,hits.hits&ignore_unavailable=true&scroll=2m&size=200",
                ESQueryUtils.getSearchUrl("http://localhost:9200", "myindex", "_doc", 200, 2, false));
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_getSearchUrl_fail()
    {
        ESQueryUtils.getSearchUrl("", "myindex", "_doc", 100, null, false);
    }

    @Test
    public void test_getScrollUrl()
    {
        assertEquals("http://localhost:9200/_search/scroll?scroll=2m&filter_path=_scroll_id,hits.hits", ESQueryUtils.getScrollUrl("http://localhost:9200", 2));
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_getScrollUrl_fail()
    {
        ESQueryUtils.getScrollUrl("", 2);
    }

    @Test
    public void test_getMgetUrl()
    {
        assertEquals("http://localhost:9200/myindex/type/_mget", ESQueryUtils.getMgetUrl("http://localhost:9200", "myindex", "type"));
        assertEquals("http://localhost:9200/myindex/_mget", ESQueryUtils.getMgetUrl("http://localhost:9200", "myindex", ""));
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_getMgetUrl_fail()
    {
        ESQueryUtils.getMgetUrl("", "", "");
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_getMgetUrl_fail_1()
    {
        ESQueryUtils.getMgetUrl("index", "", "");
    }

    @Test
    public void test_search_body_singleType_with_index()
    {
        // field1 != 20
        List<IPredicate> pairs = asList(IPredicateMock.neq("field1", 20));

        // Null outer values should yield a dummy value
        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"terms\":{\"index.keyword\":[\"<index values>\"]}}],\"must_not\":[{\"term\":{\"field1\":20}}]}}}",
                ESQueryUtils.getSearchBody(false, new GenericStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate(p.getQualifiedColumn()
                                .toString(), p, false))
                        .collect(toList()), null, "index.keyword", true, context));

        ValueVector indexSeekValues = ValueVector.literalAny((Object) 1, (Object) 2);

        // Non quote
        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"terms\":{\"index.keyword\":[1,2]}}],\"must_not\":[{\"term\":{\"field1\":20}}]}}}",
                ESQueryUtils.getSearchBody(false, new GenericStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate(p.getQualifiedColumn()
                                .toString(), p, false))
                        .collect(toList()), indexSeekValues, "index.keyword", false, context));

        // Quote
        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"terms\":{\"index.keyword\":[\"1\",\"2\"]}}],\"must_not\":[{\"term\":{\"field1\":20}}]}}}",
                ESQueryUtils.getSearchBody(false, new GenericStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate(p.getQualifiedColumn()
                                .toString(), p, false))
                        .collect(toList()), indexSeekValues, "index.keyword", true, context));
    }

    @Test
    public void test_search_body_singleType()
    {
        assertEquals("{\"sort\":[\"_doc\"]}", ESQueryUtils.getSearchBody(false, new GenericStrategy(), emptyList(), emptyList(), null, null, false, context));
        assertEquals(
                // CSOFF
                "{\"sort\":[{\"@timestamp\":{\"order\":\"desc\",\"missing\":\"_last\"}},{\"count\":{\"order\":\"asc\"}},{\"nested.object.value\":{\"order\":\"asc\",\"nested\":{\"path\":\"nested.object\"}}},{\"text.keyword\":{\"order\":\"asc\"}}]}",
                // CSON
                ESQueryUtils.getSearchBody(false, new GenericStrategy(), asList(
                        // Null order
                        sortItem("@timestamp", "int", null, Order.DESC, NullOrder.LAST),
                        // No null order
                        sortItem("count", "text", null, Order.ASC, NullOrder.UNDEFINED),
                        // Nested
                        sortItem("nested.object.value", "int", "nested.object", Order.ASC, NullOrder.UNDEFINED),
                        // Free-text property with a non free text field, then we should sort on the non-free-text variant
                        sortItem("text", "text", null, Order.ASC, NullOrder.UNDEFINED, prop("text.keyword", "keyword", null))), emptyList(), null, null, false, context));

        List<IPredicate> pairs = getMockPairs();
        assertEquals(
                // CSOFF
                "{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"wildcard\":{\"field4\":{\"value\":\"other*string?end\"}}},{\"wildcard\":{\"field3\":{\"value\":\"some*string?end\"}}},{\"terms\":{\"field\":[1,2,3]}},{\"term\":{\"count\":10}},{\"range\":{\"timestamp\":{\"lte\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"gte\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"lt\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"gt\":\"20201010T10:10:10:000Z\"}}}],\"must_not\":[{\"terms\":{\"field7\":[4,5,6]}},{\"wildcard\":{\"field6\":{\"value\":\"other?string*end\"}}},{\"wildcard\":{\"field5\":{\"value\":\"some?string*end\"}}},{\"term\":{\"field2\":true}},{\"term\":{\"field1\":20}}]}}}",
                // CSON
                ESQueryUtils.getSearchBody(false, new GenericStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate(p.getQualifiedColumn()
                                .toString(), p, false))
                        .collect(toList()), null, null, false, context));

        pairs = asList(IPredicateMock.neq("field1", 20));
        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"must_not\":[{\"term\":{\"field1\":20}}]}}}", ESQueryUtils.getSearchBody(false, new GenericStrategy(), emptyList(), pairs.stream()
                .map(p -> new PropertyPredicate(p.getQualifiedColumn()
                        .toString(), p, false))
                .collect(toList()), null, null, false, context));

        pairs = asList(IPredicateMock.notLike("field5", "some_string%end"));
        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"must_not\":[{\"wildcard\":{\"field5\":{\"value\":\"some?string*end\"}}}]}}}",
                ESQueryUtils.getSearchBody(false, new GenericStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate(p.getQualifiedColumn()
                                .toString(), p, false))
                        .collect(toList()), null, null, false, context));

        pairs = asList(IPredicateMock.like("field4", "other%string_end"));
        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"wildcard\":{\"field4\":{\"value\":\"other*string?end\"}}}]}}}",
                ESQueryUtils.getSearchBody(false, new GenericStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate(p.getQualifiedColumn()
                                .toString(), p, false))
                        .collect(toList()), null, null, false, context));

        pairs = asList(IPredicateMock.function("es", "match", asList("field4", "some phrase")));

        assertTrue(PropertyPredicate.isSupported(pairs.get(0), "es"));
        assertFalse(PropertyPredicate.isSupported(pairs.get(0), "sys"));

        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"match\":{\"field4\":\"some phrase\"}}]}}}",
                ESQueryUtils.getSearchBody(false, new GenericStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate("", p, true))
                        .collect(toList()), null, null, false, context));

        pairs = asList(IPredicateMock.function("es", "query", asList("field4:'some phrase'")));

        assertTrue(PropertyPredicate.isSupported(pairs.get(0), "es"));
        assertFalse(PropertyPredicate.isSupported(pairs.get(0), "sys"));

        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"query_string\":{\"query\":\"field4:'some phrase'\"}}]}}}",
                ESQueryUtils.getSearchBody(false, new GenericStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate("", p, true))
                        .collect(toList()), null, null, false, context));

        pairs = asList(IPredicateMock._null("field1", false), IPredicateMock._null("field2", true));
        assertEquals("{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"exists\":{\"field\":\"field2\"}}],\"must_not\":[{\"exists\":{\"field\":\"field1\"}}]}}}",
                ESQueryUtils.getSearchBody(false, new GenericStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate(p.getQualifiedColumn()
                                .toString(), p, false))
                        .collect(toList()), null, null, false, context));
    }

    @Test
    public void test_search_body_with_index()
    {
        ValueVector indexSeekValues = ValueVector.literalAny(1, 2, null);

        // field1 != 20
        List<IPredicate> pairs = asList(IPredicateMock.neq("field1", 20));
        // Non quote
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"terms\":{\"index.keyword\":[1,2]}}],\"must_not\":[{\"term\":{\"field1\":20}}]}}}",
                ESQueryUtils.getSearchBody(false, new Elastic1XStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate(p.getQualifiedColumn()
                                .toString(), p, false))
                        .collect(toList()), indexSeekValues, "index.keyword", false, context));

        // Quote
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"terms\":{\"index.keyword\":[\"1\",\"2\"]}}],\"must_not\":[{\"term\":{\"field1\":20}}]}}}",
                ESQueryUtils.getSearchBody(false, new Elastic1XStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate(p.getQualifiedColumn()
                                .toString(), p, false))
                        .collect(toList()), indexSeekValues, "index.keyword", true, context));
    }

    @Test
    public void test_search_body()
    {
        assertEquals("{\"sort\":[\"_doc\"]}", ESQueryUtils.getSearchBody(false, new Elastic1XStrategy(), emptyList(), emptyList(), null, null, false, context));
        assertEquals(
                // CSOFF
                "{\"sort\":[{\"@timestamp\":{\"order\":\"desc\",\"missing\":\"_first\"}},{\"count\":{\"order\":\"asc\"}},{\"nested.object.value\":{\"order\":\"asc\",\"nested_path\":\"nested.object\"}},{\"text.raw\":{\"order\":\"asc\"}}]}",
                // CSON
                ESQueryUtils.getSearchBody(false, new Elastic1XStrategy(), asList(
                        // Null order
                        sortItem("@timestamp", "int", null, Order.DESC, NullOrder.FIRST),
                        // No null order
                        sortItem("count", "string", null, Order.ASC, NullOrder.UNDEFINED),
                        // Nested
                        sortItem("nested.object.value", "int", "nested.object", Order.ASC, NullOrder.UNDEFINED),
                        // Free-text property with a non free text field, then we should sort on the non-free-text variant
                        sortItem("text", "string", null, Order.ASC, NullOrder.UNDEFINED, prop("text.raw", "string", "not_analyzed"))), emptyList(), null, null, false, context));

        List<IPredicate> pairs = getMockPairs();
        assertEquals(
                // CSOFF
                "{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"wildcard\":{\"field4\":{\"value\":\"other*string?end\"}}},{\"wildcard\":{\"field3\":{\"value\":\"some*string?end\"}}},{\"terms\":{\"field\":[1,2,3]}},{\"term\":{\"count\":10}},{\"range\":{\"timestamp\":{\"lte\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"gte\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"lt\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"gt\":\"20201010T10:10:10:000Z\"}}}],\"must_not\":[{\"terms\":{\"field7\":[4,5,6]}},{\"wildcard\":{\"field6\":{\"value\":\"other?string*end\"}}},{\"wildcard\":{\"field5\":{\"value\":\"some?string*end\"}}},{\"term\":{\"field2\":true}},{\"term\":{\"field1\":20}}]}}}",
                // CSON
                ESQueryUtils.getSearchBody(false, new Elastic1XStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate(p.getQualifiedColumn()
                                .toString(), p, false))
                        .collect(toList()), null, null, false, context));

        pairs = asList(IPredicateMock.neq("field1", 20));
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must_not\":[{\"term\":{\"field1\":20}}]}}}", ESQueryUtils.getSearchBody(false, new Elastic1XStrategy(), emptyList(), pairs.stream()
                .map(p -> new PropertyPredicate(p.getQualifiedColumn()
                        .toString(), p, false))
                .collect(toList()), null, null, false, context));

        pairs = asList(IPredicateMock.eq("field1", 20));
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"term\":{\"field1\":20}}]}}}", ESQueryUtils.getSearchBody(false, new Elastic1XStrategy(), emptyList(), pairs.stream()
                .map(p -> new PropertyPredicate(p.getQualifiedColumn()
                        .toString(), p, false))
                .collect(toList()), null, null, false, context));

        pairs = asList(IPredicateMock.notLike("field5", "some_string%end"));
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must_not\":[{\"wildcard\":{\"field5\":{\"value\":\"some?string*end\"}}}]}}}",
                ESQueryUtils.getSearchBody(false, new Elastic1XStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate(p.getQualifiedColumn()
                                .toString(), p, false))
                        .collect(toList()), null, null, false, context));

        pairs = asList(IPredicateMock.like("field4", "other%string_end"));
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"wildcard\":{\"field4\":{\"value\":\"other*string?end\"}}}]}}}",
                ESQueryUtils.getSearchBody(false, new Elastic1XStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate(p.getQualifiedColumn()
                                .toString(), p, false))
                        .collect(toList()), null, null, false, context));

        IColumnExpression col = mock(IColumnExpression.class);
        when(col.getColumn()).thenReturn("field4");

        pairs = asList(IPredicateMock.function("es", "match", asList(col, "some phrase")));
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"match\":{\"field4\":\"some phrase\"}}]}}}",
                ESQueryUtils.getSearchBody(false, new Elastic1XStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate("", p, true))
                        .collect(toList()), null, null, false, context));

        pairs = asList(IPredicateMock.function("es", "match", asList("field4,field5", "some phrase")));
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"multi_match\":{\"fields\":[\"field4\",\"field5\"],\"query\":\"some phrase\"}}]}}}",
                ESQueryUtils.getSearchBody(false, new Elastic1XStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate("", p, true))
                        .collect(toList()), null, null, false, context));

        pairs = asList(IPredicateMock.function("es", "query", asList("field4:'some phrase'")));
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"query_string\":{\"query\":\"field4:'some phrase'\"}}]}}}",
                ESQueryUtils.getSearchBody(false, new Elastic1XStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate("", p, true))
                        .collect(toList()), null, null, false, context));

        pairs = asList(IPredicateMock._null("field1", false), IPredicateMock._null("field2", true));
        assertEquals("{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"exists\":{\"field\":\"field2\"}}],\"must_not\":[{\"exists\":{\"field\":\"field1\"}}]}}}",
                ESQueryUtils.getSearchBody(false, new Elastic1XStrategy(), emptyList(), pairs.stream()
                        .map(p -> new PropertyPredicate(p.getQualifiedColumn()
                                .toString(), p, false))
                        .collect(toList()), null, null, false, context));
    }

    private List<IPredicate> getMockPairs()
    {
        // @formatter:off
        List<IPredicate> pairs = asList(
                IPredicateMock.like("field4", "other%string_end"),
                IPredicateMock.like("field3", "some%string_end"),
                IPredicateMock.in("field", asList(1,2,3)),
                IPredicateMock.eq("count", 10),
                IPredicateMock.lte("timestamp", "20201010T10:10:10:000Z"),
                IPredicateMock.gte("timestamp", "20201010T10:10:10:000Z"),
                IPredicateMock.lt("timestamp", "20201010T10:10:10:000Z"),
                IPredicateMock.gt("timestamp", "20201010T10:10:10:000Z"),
                IPredicateMock.notIn("field7", asList(4,5,6)),
                IPredicateMock.notLike("field6", "other_string%end"),
                IPredicateMock.notLike("field5", "some_string%end"),
                IPredicateMock.neq("field2", true),
                IPredicateMock.neq("field1", 20)
                );
        // @formatter:on
        return pairs;
    }

    private SortItemMeta sortItem(String name, String type, String nestedPath, Order order, NullOrder nullOrder)
    {
        return sortItem(name, type, nestedPath, order, nullOrder, null);
    }

    private SortItemMeta sortItem(String name, String type, String nestedPath, Order order, NullOrder nullOrder, MappedProperty field)
    {
        MappedProperty prop = new MappedProperty(QualifiedName.of(name), type, null, nestedPath != null ? QualifiedName.of(nestedPath)
                : null,
                field != null ? singletonList(field)
                        : emptyList(),
                emptyMap());
        return new SortItemMeta(prop, order, nullOrder);
    }

    static MappedProperty prop(String name, String type, Object index)
    {
        return new MappedProperty(QualifiedName.of(name), type, index, null, emptyList(), emptyMap());
    }
}
