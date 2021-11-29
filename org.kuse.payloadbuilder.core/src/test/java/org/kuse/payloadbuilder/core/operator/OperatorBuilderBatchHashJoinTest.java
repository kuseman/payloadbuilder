package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.operator.BatchCacheOperator.CacheSettings;
import org.kuse.payloadbuilder.core.operator.Operator.TupleIterator;
import org.kuse.payloadbuilder.core.operator.OperatorBuilder.BuildResult;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.Select;

/** Test of {@link OperatorBuilder} with index on tables */
public class OperatorBuilderBatchHashJoinTest extends AOperatorTest
{
    @Test
    public void test_batch_cache_asterisk_is_populated()
    {
        String query = "select a.col, b.col1 "
            + "from tableA a "
            + "inner join "
            + "("
            + "   select c.col1"
            + "   from tableB c"
            + ") b with (cacheName = 'tableB', cacheKey = @param, cacheTTL = 'PT10m') "
            + "  on b.col1 = a.col1";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(
                entry("tableB", asList("col1"))), operators);
        session.getCatalogRegistry().registerCatalog("c", c);
        session.getCatalogRegistry().setDefaultCatalog("c");

        Select select = s(query);
        OperatorBuilder.create(session, select);
    }

    @Test
    public void test_batch_cache()
    {
        String query = "select a.col, b.col "
            + "from tableA a "
            + "inner join tableB b with (cacheName = 'tableB', cacheKey = @param, cacheTTL = 'PT10m') "
            + "  on b.col1 = a.col1";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(
                entry("tableB", asList("col1"))), operators);
        session.getCatalogRegistry().registerCatalog("c", c);
        session.getCatalogRegistry().setDefaultCatalog("c");

        Select select = s(query);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(
                3,
                "INNER JOIN",
                operators.get(0),
                new BatchCacheOperator(
                        2,
                        operators.get(1),
                        new ExpressionIndexValuesFactory(asList(e("b.col1"))),
                        new CacheSettings(
                                new ExpressionFunction(e("'tableB'")),
                                new ExpressionFunction(e("@param")),
                                new ExpressionFunction(e("'PT10m'")))),
                new ExpressionIndexValuesFactory(asList(e("a.col1"))),
                new ExpressionHashFunction(asList(e("b.col1"))),
                new ExpressionPredicate(e("b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 1, 2),
                false,
                false,
                c.getIndices(session, "", QualifiedName.of("tableB")).get(0),
                null);

        assertEquals(expected, buildResult.getOperator());
    }

    @Test
    public void test_correlated_with_index_access()
    {
        String queryString = "SELECT s.art_id "
            + "FROM source s "
            + "INNER JOIN "
            + "("
            + "  select * "
            + "  from article a"
            + "  INNER JOIN articleAttribute aa with(populate=true)"
            + "    ON aa.art_id = a.art_id "
            + "    AND s.id "
            + ") a with(populate=true) "
            + "  ON a.art_id = s.art_id";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(
                entry("article", asList("art_id"))), operators);
        session.getCatalogRegistry().registerCatalog("c", c);
        session.getCatalogRegistry().setDefaultCatalog("c");

        Select select = s(queryString);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        Operator expected = new NestedLoopJoin(
                5,
                "INNER JOIN",
                operators.get(0),
                new OuterValuesOperator(
                        4,
                        new HashJoin(
                                3,
                                "INNER JOIN",
                                operators.get(1),
                                operators.get(2),
                                new ExpressionHashFunction(asList(e("a.art_id"))),
                                new ExpressionHashFunction(asList(e("aa.art_id"))),
                                new ExpressionPredicate(e("aa.art_id = a.art_id AND s.id = true")),
                                new DefaultTupleMerger(-1, 3, 2),
                                true,
                                false),
                        new ExpressionIndexValuesFactory(asList(e("s.art_id")))),
                new ExpressionPredicate(e("a.art_id = s.art_id")),
                new DefaultTupleMerger(-1, 1, 3),
                true,
                false);

        //                System.out.println(pair.getKey().toString(1));
        //                System.out.println(expected.toString(1));

        assertEquals(expected, buildResult.getOperator());
    }

    @Test
    public void test_nested_inner_join_with_pushdown()
    {
        String queryString = "SELECT a.art_id " +
            "FROM source s " +
            "INNER JOIN " +
            "(" +
            "  select * " +
            "  from article a " +
            "  INNER JOIN article_attribute aa " +
            "    ON aa.art_id = a.art_id " +
            "    AND aa.active_flg " +
            ") a with (populate=true)" +
            "  ON a.art_id = s.art_id " +
            "  AND a.club_id = 1337 + 123 " +
            "  AND a.country_id = 0 " +
            "  AND a.active_flg = 1";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(
                entry("article", asList("club_id", "country_id", "art_id")),
                entry("article_attribute", asList("art_id"))), operators);
        session.getCatalogRegistry().registerCatalog("c", c);
        session.getCatalogRegistry().setDefaultCatalog("c");

        Select select = s(queryString);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(
                6,
                "INNER JOIN",
                operators.get(0),
                new BatchHashJoin(
                        5,
                        "INNER JOIN",
                        new FilterOperator(2, operators.get(1), new ExpressionPredicate(e("a.active_flg = 1"))),
                        new FilterOperator(4, operators.get(2), new ExpressionPredicate(e("aa.active_flg = true"))),
                        new ExpressionIndexValuesFactory(asList(e("a.art_id"))),
                        new ExpressionHashFunction(asList(e("aa.art_id"))),
                        new ExpressionPredicate(e("aa.art_id = a.art_id")),
                        new DefaultTupleMerger(-1, 3, 2),
                        false,
                        false,
                        c.getIndices(session, "", QualifiedName.of("article_attribute")).get(0),
                        null),
                new ExpressionIndexValuesFactory(asList(e("1460"), e("0"), e("s.art_id"))),
                new ExpressionHashFunction(asList(e("1460"), e("0"), e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id")),
                new DefaultTupleMerger(-1, 1, 3),
                true,
                false,
                c.getIndices(session, "", QualifiedName.of("article")).get(0),
                null);

        Operator actual = buildResult.getOperator();

        //                System.out.println(actual.toString(1));
        //                System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        TupleIterator it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_inner_join_with_pushdown()
    {
        String queryString = "SELECT a.art_id " +
            "FROM source s " +
            "INNER JOIN article a " +
            "  ON a.art_id = s.art_id " +
            "  AND a.club_id = 1337 + 123 " +
            "  AND a.country_id = 0 " +
            "  AND a.active_flg = 1";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", asList("club_id", "country_id", "art_id"))), operators);
        session.getCatalogRegistry().registerCatalog("c", c);
        session.getCatalogRegistry().setDefaultCatalog("c");

        Select select = s(queryString);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(
                3,
                "INNER JOIN",
                operators.get(0),
                new FilterOperator(2, operators.get(1), new ExpressionPredicate(e("a.active_flg = 1"))),
                new ExpressionIndexValuesFactory(asList(e("1460"), e("0"), e("s.art_id"))),
                new ExpressionHashFunction(asList(e("1460"), e("0"), e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id")),
                new DefaultTupleMerger(-1, 1, 2),
                false,
                false,
                c.getIndices(session, "", QualifiedName.of("article")).get(0),
                null);

        Operator actual = buildResult.getOperator();

        //        System.out.println(actual.toString(1));
        //        System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        TupleIterator it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_inner_join_with_populate_and_filter_and_join_pushdown()
    {
        String queryString = "SELECT a.art_id " +
            "FROM source s " +
            "INNER JOIN (select * from article where internet_flg = 1) a with(populate=true)" +
            "  ON a.art_id = s.art_id " +
            "  AND a.club_id = 1337 + 123 " +
            "  AND a.country_id = 0 " +
            "  AND a.active_flg = 1";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", asList("club_id", "country_id", "art_id"))), operators);
        session.getCatalogRegistry().registerCatalog("c", c);
        session.getCatalogRegistry().setDefaultCatalog("c");

        Select select = s(queryString);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(
                3,
                "INNER JOIN",
                operators.get(0),
                new FilterOperator(2, operators.get(1), new ExpressionPredicate(e("internet_flg = 1 AND a.active_flg = 1"))),
                new ExpressionIndexValuesFactory(asList(e("1460"), e("0"), e("s.art_id"))),
                new ExpressionHashFunction(asList(e("1460"), e("0"), e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id")),
                new DefaultTupleMerger(-1, 1, 2),
                true,
                false,
                c.getIndices(session, "", QualifiedName.of("article")).get(0),
                null);

        Operator actual = buildResult.getOperator();

        //        System.out.println(actual.toString(1));
        //        System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        TupleIterator it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_left_join_with_populate_and_filter_doesnt_pushdown_predicate()
    {
        String queryString = "SELECT a.art_id " +
            "FROM source s " +
            "LEFT JOIN "
            + "("
            + "  select * "
            + "from article "
            + "where internet_flg = 1"
            + ") a with (populate=true)" +
            "  ON a.art_id = s.art_id " +
            "  AND a.club_id = 1337 + 123 " +
            "  AND a.country_id = 0 " +
            "  AND a.active_flg = 1" +
            "  AND s.flag";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", asList("club_id", "country_id", "art_id"))), operators);
        session.getCatalogRegistry().registerCatalog("c", c);
        session.getCatalogRegistry().setDefaultCatalog("c");

        Select select = s(queryString);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(
                3,
                "LEFT JOIN",
                operators.get(0),
                new FilterOperator(2, operators.get(1), new ExpressionPredicate(e("internet_flg = 1 AND a.active_flg = 1"))),
                new ExpressionIndexValuesFactory(asList(e("1460"), e("0"), e("s.art_id"))),
                new ExpressionHashFunction(asList(e("1460"), e("0"), e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id AND s.flag = true")),
                new DefaultTupleMerger(-1, 1, 2),
                true,
                true,
                c.getIndices(session, "", QualifiedName.of("article")).get(0),
                null);

        Operator actual = buildResult.getOperator();

        //        System.out.println(actual.toString(1));
        //        System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        TupleIterator it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_left_join_pushdown_predicate()
    {
        String queryString = "SELECT a.art_id " +
            "FROM source s " +
            "LEFT JOIN article a " +
            "  ON a.art_id = s.art_id " +
            "  AND a.club_id = 1337 + 123 " +
            "  AND a.country_id = 0 " +
            "  AND a.active_flg = 1";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", asList("club_id", "country_id", "art_id"))), operators);
        session.getCatalogRegistry().registerCatalog("c", c);
        session.getCatalogRegistry().setDefaultCatalog("c");

        Select select = s(queryString);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(
                3,
                "LEFT JOIN",
                operators.get(0),
                new FilterOperator(2, operators.get(1), new ExpressionPredicate(e("a.active_flg = 1"))),
                new ExpressionIndexValuesFactory(asList(e("1460"), e("0"), e("s.art_id"))),
                new ExpressionHashFunction(asList(e("1460"), e("0"), e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id")),
                new DefaultTupleMerger(-1, 1, 2),
                false,
                true,
                c.getIndices(session, "", QualifiedName.of("article")).get(0),
                null);

        Operator actual = buildResult.getOperator();

        //                System.out.println(actual.toString(1));
        //                System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        TupleIterator it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    private Catalog catalog(
            Map<String, List<String>> keysByTable,
            List<Operator> operators)
    {
        return new Catalog("TEST")
        {
            @Override
            public List<Index> getIndices(QuerySession session, String catalogAlias, QualifiedName table)
            {
                List<String> keys = keysByTable.get(table.toString());
                return keys != null ? asList(new Index(table, keys, 100)) : emptyList();
            }

            @Override
            public Operator getScanOperator(OperatorData data)
            {
                Operator op = new TestOperator("scan " + data.getTableAlias().getTable().toString(), keysByTable);
                operators.add(op);
                return op;
            }

            @Override
            public Operator getIndexOperator(OperatorData data, Index index)
            {
                Operator op = new TestOperator("index " + data.getTableAlias().getTable().toString(), keysByTable);
                operators.add(op);
                return op;
            }
        };
    }

    /** Test operator */
    private static class TestOperator implements Operator
    {
        private final String name;
        private final Map<String, List<String>> keysByTable;

        TestOperator(String name, Map<String, List<String>> keysByTable)
        {
            this.name = name;
            this.keysByTable = keysByTable;
        }

        @Override
        public int getNodeId()
        {
            return 0;
        }

        @Override
        public TupleIterator open(ExecutionContext context)
        {
            if (keysByTable.containsKey(name))
            {
                assertNotNull(context.getStatementContext().getOuterIndexValues());
            }
            return TupleIterator.EMPTY;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }
}
