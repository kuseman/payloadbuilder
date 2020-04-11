package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.operator.CachingOperator;
import com.viskan.payloadbuilder.operator.DefaultRowMerger;
import com.viskan.payloadbuilder.operator.ExpressionProjection;
import com.viskan.payloadbuilder.operator.NestedLoop;
import com.viskan.payloadbuilder.operator.ObjectProjection;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.RowSpool;
import com.viskan.payloadbuilder.operator.RowSpoolScan;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;

import org.junit.Test;

/** Test of {@link OperatorBuilder} building applys */
public class OperatorBuilderApplyTest extends OperatorBuilderTest
{
    @Test
    public void test_cross_apply()
    {
        String query = "select s.id1, a.id2 from source s cross apply article a";
        QueryResult result = getQueryResult(query);

        // Assert aliaes
        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"id1"});
        new TableAlias(source, QualifiedName.of("article"), "a", new String[] {"id2"});

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.aliases.get(0)));

        Operator expected = new RowSpool("parents", result.tableOperators.get(0),
                new NestedLoop(
                        "",
                        new RowSpoolScan("parents"),
                        new CachingOperator(result.tableOperators.get(1)),
                        null,
                        DefaultRowMerger.DEFAULT,
                        false,
                        false));

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(ofEntries(true,
                        entry("id1", new ExpressionProjection(e("s.id1"))),
                        entry("id2", new ExpressionProjection(e("a.id2"))))),
                result.projection);
    }

    @Test
    public void test_cross_apply_with_populate()
    {
        String query = "select s.id1, a.id2 from source s cross apply [article] a";
        QueryResult result = getQueryResult(query);

        // Assert aliaes
        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"id1"});
        new TableAlias(source, QualifiedName.of("article"), "a", new String[] {"id2"});

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.aliases.get(0)));

        Operator expected = new RowSpool("parents", result.tableOperators.get(0),
                new NestedLoop(
                        "",
                        new RowSpoolScan("parents"),
                        new CachingOperator(result.tableOperators.get(1)),
                        null,
                        DefaultRowMerger.DEFAULT,
                        true,
                        false));

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(ofEntries(true,
                        entry("id1", new ExpressionProjection(e("s.id1"))),
                        entry("id2", new ExpressionProjection(e("a.id2"))))),
                result.projection);
    }

    @Test
    public void test_outer_apply()
    {
        String query = "select s.id1, a.id2 from source s outer apply article a";
        QueryResult result = getQueryResult(query);

        // Assert aliaes
        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"id1"});
        new TableAlias(source, QualifiedName.of("article"), "a", new String[] {"id2"});

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.aliases.get(0)));

        Operator expected = new RowSpool("parents", result.tableOperators.get(0),
                new NestedLoop(
                        "",
                        new RowSpoolScan("parents"),
                        new CachingOperator(result.tableOperators.get(1)),
                        null,
                        DefaultRowMerger.DEFAULT,
                        false,
                        true));

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(ofEntries(true,
                        entry("id1", new ExpressionProjection(e("s.id1"))),
                        entry("id2", new ExpressionProjection(e("a.id2"))))),
                result.projection);
    }

    @Test
    public void test_outer_apply_with_populate()
    {
        String query = "select s.id1, a.id2 from source s outer apply [article] a";
        QueryResult result = getQueryResult(query);

        // Assert aliaes
        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"id1"});
        new TableAlias(source, QualifiedName.of("article"), "a", new String[] {"id2"});

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.aliases.get(0)));

        Operator expected = new RowSpool("parents", result.tableOperators.get(0),
                new NestedLoop(
                        "",
                        new RowSpoolScan("parents"),
                        new CachingOperator(result.tableOperators.get(1)),
                        null,
                        DefaultRowMerger.DEFAULT,
                        true,
                        true));

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(ofEntries(true,
                        entry("id1", new ExpressionProjection(e("s.id1"))),
                        entry("id2", new ExpressionProjection(e("a.id2"))))),
                result.projection);
    }
}
