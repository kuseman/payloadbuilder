package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.operator.TableAlias.Type;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.Table;

/** Test building of nested loops */
public class OperatorBuilderNestedLoopJoinTest extends AOperatorTest
{
    @Test
    public void test_nested_loop_no_cache_operator_when_inner_is_temporary_table()
    {
        String query = "select s.id1, t1.id2 "
            + "from source s "
            + "inner join #temp t1 "
            + "  on t1.col = s.id3 "
            + "  or active";

        TableAlias tempAlias = TableAliasBuilder.of(1, Type.TEMPORARY_TABLE, QualifiedName.of("temp"), "t1").build();
        session.setTemporaryTable(new TemporaryTable(QualifiedName.of("temp"), tempAlias, new String[] { "col", "active", "id2" }, emptyList()));

        QueryResult result = getQueryResult(query);

        Operator expected = new NestedLoopJoin(
                2,
                "INNER JOIN",
                result.tableOperators.get(0),
                new TemporaryTableScanOperator(1, new Table(null, tempAlias, emptyList(), null)),
                new ExpressionPredicate(e("t1.col = s.id3 or active")),
                new DefaultTupleMerger(-1, 1, 2),
                false,
                false);

        //        System.out.println(expected.toString(1));
        //        System.err.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(new RootProjection(asList("id1", "id2"),
                asList(
                        new ExpressionProjection(e("s.id1")),
                        new ExpressionProjection(e("t1.id2")))),
                result.projection);
    }

    @Test
    public void test_nested_loop_cache_operator_when_inner_is_filtered_temporary_table()
    {
        String query = "select s.id1, t1.id2 from source s inner join #temp t1 on (t1.col = s.id3 or active) and t1.col > 10";

        TableAlias tempAlias = TableAliasBuilder.of(1, Type.TEMPORARY_TABLE, QualifiedName.of("temp"), "t1").build();
        session.setTemporaryTable(new TemporaryTable(QualifiedName.of("temp"), tempAlias, new String[] { "col", "active", "id2" }, emptyList()));
        QueryResult result = getQueryResult(query);

        Operator expected = new NestedLoopJoin(
                4,
                "INNER JOIN",
                result.tableOperators.get(0),
                new CachingOperator(3,
                        new FilterOperator(2,
                                new TemporaryTableScanOperator(1, new Table(null, tempAlias, emptyList(), null)),
                                new ExpressionPredicate(e("t1.col > 10")))),
                new ExpressionPredicate(e("t1.col = s.id3 or active")),
                new DefaultTupleMerger(-1, 1, 2),
                false,
                false);

        //        System.out.println(expected.toString(1));
        //        System.err.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(new RootProjection(asList("id1", "id2"),
                asList(
                        new ExpressionProjection(e("s.id1")),
                        new ExpressionProjection(e("t1.id2")))),
                result.projection);
    }

    @Test
    public void test_nested_loop_with_pushdown()
    {
        String query = "select s.id1, a.id2 from source s inner join article a on a.active_flg and (a.art_id = s.art_id or s.id1 > 0)";
        QueryResult result = getQueryResult(query);

        Operator expected = new NestedLoopJoin(
                4,
                "",
                result.tableOperators.get(0),
                new CachingOperator(3, new FilterOperator(2, result.tableOperators.get(1), new ExpressionPredicate(e("a.active_flg = true")))),
                new ExpressionPredicate(e("a.art_id = s.art_id or s.id1 > 0")),
                new DefaultTupleMerger(-1, 1, 2),
                false,
                false);

        //        System.out.println(expected.toString(1));
        //        System.err.println( result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(new RootProjection(asList("id1", "id2"),
                asList(
                        new ExpressionProjection(e("s.id1")),
                        new ExpressionProjection(e("a.id2")))),
                result.projection);
    }

    @Test
    public void test_nested_loop_with_populate_and_pushdown()
    {
        String query = "select s.id1, a.id2 from source s inner join (select * from article a where a.internet_flg ) a with(populate=true) on a.active_flg and (a.art_id = s.art_id or s.id1 > 0)";
        QueryResult result = getQueryResult(query);

        Operator expected = new NestedLoopJoin(
                4,
                "INNER JOIN",
                result.tableOperators.get(0),
                new CachingOperator(3,
                        new FilterOperator(2, result.tableOperators.get(1), new ExpressionPredicate(e("a.internet_flg = true AND a.active_flg = true")))),
                new ExpressionPredicate(e("a.art_id = s.art_id or s.id1 > 0")),
                new DefaultTupleMerger(-1, 1, 2),
                true,
                false);

        //        System.out.println(expected.toString(1));
        //        System.err.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(new RootProjection(asList("id1", "id2"),
                asList(
                        new ExpressionProjection(e("s.id1")),
                        new ExpressionProjection(e("a.id2")))),
                result.projection);
    }
}
