package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.operator.CachingOperator;
import org.kuse.payloadbuilder.core.operator.DefaultRowMerger;
import org.kuse.payloadbuilder.core.operator.ExpressionProjection;
import org.kuse.payloadbuilder.core.operator.NestedLoopJoin;
import org.kuse.payloadbuilder.core.operator.ObjectProjection;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.OperatorBuilder;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Test of {@link OperatorBuilder} building applys */
public class OperatorBuilderApplyTest extends AOperatorTest
{
    @Test
    public void test_cross_apply()
    {
        String query = "select s.id1, a.id2 from source s cross apply article a";
        QueryResult result = getQueryResult(query);

        // Assert aliaes
        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"id1"});
        new TableAlias(source, QualifiedName.of("article"), "a", new String[] {"id2"});

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.alias));

        Operator expected = new NestedLoopJoin(
                3,
                "",
                result.tableOperators.get(0),
                new CachingOperator(2, result.tableOperators.get(1)),
                null,
                DefaultRowMerger.DEFAULT,
                false,
                false);

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(asList("id1", "id2"),
                        asList(
                                new ExpressionProjection(e("s.id1")),
                                new ExpressionProjection(e("a.id2")))),
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

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.alias));

        Operator expected = new NestedLoopJoin(
                3,
                "",
                result.tableOperators.get(0),
                new CachingOperator(2, result.tableOperators.get(1)),
                null,
                DefaultRowMerger.DEFAULT,
                true,
                false);

        //        System.out.println(expected.toString(1));
        //        System.err.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(asList("id1", "id2"),
                        asList(
                                new ExpressionProjection(e("s.id1")),
                                new ExpressionProjection(e("a.id2")))),
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

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.alias));

        Operator expected = new NestedLoopJoin(
                3,
                "",
                result.tableOperators.get(0),
                new CachingOperator(2, result.tableOperators.get(1)),
                null,
                DefaultRowMerger.DEFAULT,
                false,
                true);

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(asList("id1", "id2"),
                        asList(
                                new ExpressionProjection(e("s.id1")),
                                new ExpressionProjection(e("a.id2")))),
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

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.alias));

        Operator expected = new NestedLoopJoin(
                3,
                "",
                result.tableOperators.get(0),
                new CachingOperator(2, result.tableOperators.get(1)),
                null,
                DefaultRowMerger.DEFAULT,
                true,
                true);

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(asList("id1", "id2"),
                        asList(
                                new ExpressionProjection(e("s.id1")),
                                new ExpressionProjection(e("a.id2")))),
                result.projection);
    }
}
