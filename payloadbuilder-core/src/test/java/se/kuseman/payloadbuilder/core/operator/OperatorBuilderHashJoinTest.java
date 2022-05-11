package se.kuseman.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.operator.Operator;

/** Test of {@link OperatorBuilder} building hash match (joins) */
public class OperatorBuilderHashJoinTest extends AOperatorTest
{
    @Test
    public void test_hash_join()
    {
        String query = "select s.id1, a.id2 from source s inner join article a on s.art_id = a.art_id";
        QueryResult result = getQueryResult(query);

        Operator expected = new HashJoin(2, "", result.tableOperators.get(0), result.tableOperators.get(1), new ExpressionHashFunction(asList(en("s.art_id"))),
                new ExpressionHashFunction(asList(en("a.art_id"))), new ExpressionPredicate(en("s.art_id = a.art_id")), new DefaultTupleMerger(-1, 1, 2), false, false);

        // System.out.println(expected.toString(1));
        // System.out.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(new RootProjection(asList("id1", "id2"), asList(new ExpressionProjection(en("s.id1")), new ExpressionProjection(en("a.id2")))), result.projection);
    }

    @Test
    public void test_hash_join_populate()
    {
        String query = "select s.id1, a.id2 from source s inner join article a with(populate=true) on a.art_id = s.art_id";
        QueryResult result = getQueryResult(query);

        Operator expected = new HashJoin(2, "", result.tableOperators.get(0), result.tableOperators.get(1), new ExpressionHashFunction(asList(en("s.art_id"))),
                new ExpressionHashFunction(asList(en("a.art_id"))), new ExpressionPredicate(en("a.art_id = s.art_id")), new DefaultTupleMerger(-1, 1, 2), true, false);

        // System.err.println(expected.toString(1));
        // System.out.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(new RootProjection(asList("id1", "id2"), asList(new ExpressionProjection(en("s.id1")), new ExpressionProjection(en("a.id2")))), result.projection);
    }

    @Test
    public void test_left_hash_join()
    {
        String query = "select s.id1, a.id2 from source s left join article a on s.art_id = a.art_id";
        QueryResult result = getQueryResult(query);

        Operator expected = new HashJoin(2, "", result.tableOperators.get(0), result.tableOperators.get(1), new ExpressionHashFunction(asList(en("s.art_id"))),
                new ExpressionHashFunction(asList(en("a.art_id"))), new ExpressionPredicate(en("s.art_id = a.art_id")), new DefaultTupleMerger(-1, 1, 2), false, true);

        // System.err.println(expected.toString(1));
        // System.out.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(new RootProjection(asList("id1", "id2"), asList(new ExpressionProjection(en("s.id1")), new ExpressionProjection(en("a.id2")))), result.projection);
    }

    @Test
    public void test_left_hash_join_no_pushdown_when_is_null()
    {
        String query = "select s.id1, a.id2 from source s left join article a on s.art_id = a.art_id where a.value is null";
        QueryResult result = getQueryResult(query);

        Operator expected = new FilterOperator(
                3, new HashJoin(2, "", result.tableOperators.get(0), result.tableOperators.get(1), new ExpressionHashFunction(asList(en("s.art_id"))),
                        new ExpressionHashFunction(asList(en("a.art_id"))), new ExpressionPredicate(en("s.art_id = a.art_id")), new DefaultTupleMerger(-1, 1, 2), false, true),
                new ExpressionPredicate(en("a.value is null")));

        // System.err.println(expected.toString(1));
        // System.out.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(new RootProjection(asList("id1", "id2"), asList(new ExpressionProjection(en("s.id1")), new ExpressionProjection(en("a.id2")))), result.projection);
    }

    @Test
    public void test_left_hash_join_with_populate()
    {
        String query = "select s.id1, a.id2 from source s left join article a with(populate=true) on s.art_id = a.art_id";
        QueryResult result = getQueryResult(query);

        Operator expected = new HashJoin(2, "", result.tableOperators.get(0), result.tableOperators.get(1), new ExpressionHashFunction(asList(en("s.art_id"))),
                new ExpressionHashFunction(asList(en("a.art_id"))), new ExpressionPredicate(en("s.art_id = a.art_id")), new DefaultTupleMerger(-1, 1, 2), true, true);

        assertEquals(expected, result.operator);

        assertEquals(new RootProjection(asList("id1", "id2"), asList(new ExpressionProjection(en("s.id1")), new ExpressionProjection(en("a.id2")))), result.projection);
    }

    @Test
    public void test_inner_hash_join_with_filter()
    {
        String query = "select s.id1, a.id2 " + "from source s "
                       + "inner join "
                       + "("
                       + "  select * "
                       + "  from article "
                       + "  where note_id > 0"
                       + ") a with (populate=true) "
                       + "  on a.art_id = s.art_id "
                       + "  and a.active_flg "
                       + "where s.id3 > 0 "
                       + "and a.id2 = 10";
        QueryResult result = getQueryResult(query);

        Operator expected = new HashJoin(4, "INNER JOIN", new FilterOperator(1, result.tableOperators.get(0), new ExpressionPredicate(en("s.id3 > 0"))),
                new FilterOperator(3, result.tableOperators.get(1), new ExpressionPredicate(en("note_id > 0 AND a.active_flg = true AND a.id2 = 10"))),
                new ExpressionHashFunction(asList(en("s.art_id"))), new ExpressionHashFunction(asList(en("a.art_id"))), new ExpressionPredicate(en("a.art_id = s.art_id ")),
                new DefaultTupleMerger(-1, 1, 2), true, false);

        // System.err.println(expected.toString(1));
        // System.out.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(new RootProjection(asList("id1", "id2"), asList(new ExpressionProjection(en("s.id1")), new ExpressionProjection(en("a.id2")))), result.projection);
    }
}