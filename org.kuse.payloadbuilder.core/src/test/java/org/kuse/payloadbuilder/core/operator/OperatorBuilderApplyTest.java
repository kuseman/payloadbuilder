package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import org.junit.Test;

/** Test of {@link OperatorBuilder} building applys */
public class OperatorBuilderApplyTest extends AOperatorTest
{
    @Test
    public void test_cross_apply()
    {
        String query = "select s.id1, a.id2 from source s cross apply article a";
        QueryResult result = getQueryResult(query);

        Operator expected = new NestedLoopJoin(
                3,
                "",
                result.tableOperators.get(0),
                new CachingOperator(2, result.tableOperators.get(1)),
                null,
                new DefaultTupleMerger(-1, 1),
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
        String query = "select s.id1, a.id2 from source s cross apply article a with(populate=true)";
        QueryResult result = getQueryResult(query);

        Operator expected = new NestedLoopJoin(
                3,
                "",
                result.tableOperators.get(0),
                new CachingOperator(2, result.tableOperators.get(1)),
                null,
                new DefaultTupleMerger(-1, 1),
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

        Operator expected = new NestedLoopJoin(
                3,
                "",
                result.tableOperators.get(0),
                new CachingOperator(2, result.tableOperators.get(1)),
                null,
                new DefaultTupleMerger(-1, 1),
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
    public void test_outer_apply_with_pushdown()
    {
        String query = "select s.id1, a.id2 from source s outer apply article a where a.active_flg";
        QueryResult result = getQueryResult(query);

        Operator expected = new NestedLoopJoin(
                4,
                "OUTER APPLY",
                result.tableOperators.get(0),
                new CachingOperator(
                        3,
                        new FilterOperator(
                                2,
                                result.tableOperators.get(1),
                                new ExpressionPredicate(e("a.active_flg = true")))),
                null,
                new DefaultTupleMerger(-1, 1),
                false,
                true);

        //                System.out.println(expected.toString(1));
        //                System.err.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(asList("id1", "id2"),
                        asList(
                                new ExpressionProjection(e("s.id1")),
                                new ExpressionProjection(e("a.id2")))),
                result.projection);
    }

    @Test
    public void test_outer_apply_not_pushdown_is_null()
    {
        String query = "select s.id1, a.id2 from source s outer apply article a where a.active_flg and a.value is null";
        QueryResult result = getQueryResult(query);

        Operator expected = new FilterOperator(
                5,
                new NestedLoopJoin(
                        4,
                        "OUTER APPLY",
                        result.tableOperators.get(0),
                        new CachingOperator(
                                3,
                                new FilterOperator(
                                        2,
                                        result.tableOperators.get(1),
                                        new ExpressionPredicate(e("a.active_flg = true")))),
                        null,
                        new DefaultTupleMerger(-1, 1),
                        false,
                        true),
                new ExpressionPredicate(e("a.value is null")));

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
    public void test_outer_apply_with_populate()
    {
        String query = "select s.id1, a.id2 from source s outer apply article a with (populate=true)";
        QueryResult result = getQueryResult(query);

        Operator expected = new NestedLoopJoin(
                3,
                "",
                result.tableOperators.get(0),
                new CachingOperator(2, result.tableOperators.get(1)),
                null,
                new DefaultTupleMerger(-1, 1),
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
