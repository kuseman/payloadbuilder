package org.kuse.payloadbuilder.catalog.es;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzeResult;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.TableAlias.Type;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QueryParser;

/** Test of {@link ESUtils} */
public class ESUtilsTest extends Assert
{
    private final CatalogRegistry registry = new CatalogRegistry();
    private final ExecutionContext context = new ExecutionContext(new QuerySession(registry));
    private final QueryParser parser = new QueryParser();

    @Before
    public void setup()
    {
        registry.registerCatalog("es", new ESCatalog());
        registry.setDefaultCatalog("es");
    }

    @Test
    public void test_search_body_singleType()
    {
        assertEquals("{\"sort\":[\"_doc\"]}", ESUtils.getSearchBody(emptyList(), emptyList(), true, context));
        assertEquals("{\"sort\":[{\"@timestamp\":{\"order\":\"desc\"}},{\"count\":{\"order\":\"asc\"}}]}",
                ESUtils.getSearchBody(asList(Pair.of("@timestamp", "desc"), Pair.of("count", "asc")), emptyList(), true, context));

        TableAlias alias = TableAlias.TableAliasBuilder.of(Type.TABLE, QualifiedName.of("table"), "t").build();
        AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(parser.parseExpression(registry,
                "timestamp > '20201010T10:10:10:000Z' " +
                    "and timestamp < '20201010T10:10:10:000Z' " +
                    "and timestamp >= '20201010T10:10:10:000Z' " +
                    "and timestamp <= '20201010T10:10:10:000Z' " +
                    "and count = 10 " +
                    "and field in (1,2,3) " +
                    "and field1 != 20 " +
                    "and field2 != true " +
                    "and field3 like 'some%string_end'" +
                    "and field4 like 'other%string_end'" +
                    "and field5 not like 'some_string%end'" +
                    "and field6 not like 'other_string%end'" +
                    "and field7 not in ('a', 'b', 'c')"),
                alias);

        assertEquals(
                "{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"wildcard\":{\"field4\":{\"value\":\"other*string?end\"}}},{\"wildcard\":{\"field3\":{\"value\":\"some*string?end\"}}},{\"terms\":{\"field\":[1,2,3]}},{\"term\":{\"count\":10}},{\"range\":{\"timestamp\":{\"lte\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"gte\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"lt\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"gt\":\"20201010T10:10:10:000Z\"}}}],\"must_not\":[{\"terms\":{\"field7\":[\"a\",\"b\",\"c\"]}},{\"wildcard\":{\"field6\":{\"value\":\"other?string*end\"}}},{\"wildcard\":{\"field5\":{\"value\":\"some?string*end\"}}},{\"term\":{\"field2\":true}},{\"term\":{\"field1\":20}}]}}}",
                ESUtils.getSearchBody(
                        emptyList(),
                        analyzeResult.getPairs().stream().map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false)).collect(toList()),
                        true,
                        context));

        analyzeResult = PredicateAnalyzer.analyze(parser.parseExpression(registry,
                "field1 != 20 "),
                alias);

        assertEquals(
                "{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"must_not\":[{\"term\":{\"field1\":20}}]}}}",
                ESUtils.getSearchBody(
                        emptyList(),
                        analyzeResult.getPairs().stream().map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false)).collect(toList()),
                        true,
                        context));

        analyzeResult = PredicateAnalyzer.analyze(parser.parseExpression(registry,
                "field5 not like 'some_string%end'"),
                alias);

        assertEquals(
                "{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"must_not\":[{\"wildcard\":{\"field5\":{\"value\":\"some?string*end\"}}}]}}}",
                ESUtils.getSearchBody(
                        emptyList(),
                        analyzeResult.getPairs().stream().map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false)).collect(toList()),
                        true,
                        context));

        analyzeResult = PredicateAnalyzer.analyze(parser.parseExpression(registry,
                "field4 like 'other%string_end'"),
                alias);

        assertEquals(
                "{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"wildcard\":{\"field4\":{\"value\":\"other*string?end\"}}}]}}}",
                ESUtils.getSearchBody(
                        emptyList(),
                        analyzeResult.getPairs().stream().map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false)).collect(toList()),
                        true,
                        context));

        analyzeResult = PredicateAnalyzer.analyze(parser.parseExpression(registry,
                "match(field4, 'some phrase')"),
                alias);

        assertEquals(
                "{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"match\":{\"field4\":\"some phrase\"}}]}}}",
                ESUtils.getSearchBody(
                        emptyList(),
                        analyzeResult.getPairs().stream().map(p -> new PropertyPredicate("t", "", p, true)).collect(toList()),
                        true,
                        context));

        analyzeResult = PredicateAnalyzer.analyze(parser.parseExpression(registry,
                "query('field4:''some phrase''')"),
                alias);

        assertEquals(
                "{\"sort\":[\"_doc\"],\"query\":{\"bool\":{\"filter\":[{\"query_string\":{\"query\":\"field4:'some phrase'\"}}]}}}",
                ESUtils.getSearchBody(
                        emptyList(),
                        analyzeResult.getPairs().stream().map(p -> new PropertyPredicate("t", "", p, true)).collect(toList()),
                        true,
                        context));
    }

    @Test
    public void test_search_body()
    {
        assertEquals("{\"sort\":[\"_doc\"]}", ESUtils.getSearchBody(emptyList(), emptyList(), false, context));
        assertEquals("{\"sort\":[{\"@timestamp\":{\"order\":\"desc\"}},{\"count\":{\"order\":\"asc\"}}]}",
                ESUtils.getSearchBody(asList(Pair.of("@timestamp", "desc"), Pair.of("count", "asc")), emptyList(), false, context));

        TableAlias alias = TableAlias.TableAliasBuilder.of(Type.TABLE, QualifiedName.of("table"), "t").build();
        AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(parser.parseExpression(registry,
                "timestamp > '20201010T10:10:10:000Z' " +
                    "and timestamp < '20201010T10:10:10:000Z' " +
                    "and timestamp >= '20201010T10:10:10:000Z' " +
                    "and timestamp <= '20201010T10:10:10:000Z' " +
                    "and count = 10 " +
                    "and field in (1,2,3) " +
                    "and field1 != 20 " +
                    "and field2 != true " +
                    "and field3 like 'some%string_end'" +
                    "and field4 like 'other%string_end'" +
                    "and field5 not like 'some_string%end'" +
                    "and field6 not like 'other_string%end'" +
                    "and field7 not in ('a', 'b', 'c')"),
                alias);

        assertEquals(
                "{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"wildcard\":{\"field4\":{\"value\":\"other*string?end\"}}},{\"wildcard\":{\"field3\":{\"value\":\"some*string?end\"}}},{\"terms\":{\"field\":[1,2,3]}},{\"term\":{\"count\":10}},{\"range\":{\"timestamp\":{\"lte\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"gte\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"lt\":\"20201010T10:10:10:000Z\"}}},{\"range\":{\"timestamp\":{\"gt\":\"20201010T10:10:10:000Z\"}}}],\"must_not\":[{\"terms\":{\"field7\":[\"a\",\"b\",\"c\"]}},{\"wildcard\":{\"field6\":{\"value\":\"other?string*end\"}}},{\"wildcard\":{\"field5\":{\"value\":\"some?string*end\"}}},{\"term\":{\"field2\":true}},{\"term\":{\"field1\":20}}]}}}",
                ESUtils.getSearchBody(
                        emptyList(),
                        analyzeResult.getPairs().stream().map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false)).collect(toList()),
                        false,
                        context));

        analyzeResult = PredicateAnalyzer.analyze(parser.parseExpression(registry,
                "field1 != 20 "),
                alias);

        assertEquals(
                "{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must_not\":[{\"term\":{\"field1\":20}}]}}}",
                ESUtils.getSearchBody(
                        emptyList(),
                        analyzeResult.getPairs().stream().map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false)).collect(toList()),
                        false,
                        context));

        analyzeResult = PredicateAnalyzer.analyze(parser.parseExpression(registry,
                "field1 = 20 "),
                alias);

        assertEquals(
                "{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"term\":{\"field1\":20}}]}}}",
                ESUtils.getSearchBody(
                        emptyList(),
                        analyzeResult.getPairs().stream().map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false)).collect(toList()),
                        false,
                        context));

        analyzeResult = PredicateAnalyzer.analyze(parser.parseExpression(registry,
                "field5 not like 'some_string%end'"),
                alias);

        assertEquals(
                "{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must_not\":[{\"wildcard\":{\"field5\":{\"value\":\"some?string*end\"}}}]}}}",
                ESUtils.getSearchBody(
                        emptyList(),
                        analyzeResult.getPairs().stream().map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false)).collect(toList()),
                        false,
                        context));

        analyzeResult = PredicateAnalyzer.analyze(parser.parseExpression(registry,
                "field4 like 'other%string_end'"),
                alias);

        assertEquals(
                "{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"wildcard\":{\"field4\":{\"value\":\"other*string?end\"}}}]}}}",
                ESUtils.getSearchBody(
                        emptyList(),
                        analyzeResult.getPairs().stream().map(p -> new PropertyPredicate("t", p.getColumn("t"), p, false)).collect(toList()),
                        false,
                        context));

        analyzeResult = PredicateAnalyzer.analyze(parser.parseExpression(registry,
                "match('field4', 'some phrase')"),
                alias);

        assertEquals(
                "{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"match\":{\"field4\":\"some phrase\"}}]}}}",
                ESUtils.getSearchBody(
                        emptyList(),
                        analyzeResult.getPairs().stream().map(p -> new PropertyPredicate("t", "", p, true)).collect(toList()),
                        false,
                        context));

        analyzeResult = PredicateAnalyzer.analyze(parser.parseExpression(registry,
                "match('field4,field5', 'some phrase')"),
                alias);

        assertEquals(
                "{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"multi_match\":{\"fields\":[\"field4\",\"field5\"],\"query\":\"some phrase\"}}]}}}",
                ESUtils.getSearchBody(
                        emptyList(),
                        analyzeResult.getPairs().stream().map(p -> new PropertyPredicate("t", "", p, true)).collect(toList()),
                        false,
                        context));

        analyzeResult = PredicateAnalyzer.analyze(parser.parseExpression(registry,
                "query('field4:''some phrase''')"),
                alias);

        assertEquals(
                "{\"sort\":[\"_doc\"],\"filter\":{\"bool\":{\"must\":[{\"query_string\":{\"query\":\"field4:'some phrase'\"}}]}}}",
                ESUtils.getSearchBody(
                        emptyList(),
                        analyzeResult.getPairs().stream().map(p -> new PropertyPredicate("t", "", p, true)).collect(toList()),
                        false,
                        context));
    }
}
