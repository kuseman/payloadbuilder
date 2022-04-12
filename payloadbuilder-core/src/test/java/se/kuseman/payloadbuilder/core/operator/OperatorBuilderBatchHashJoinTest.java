package se.kuseman.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Index.ColumnsType;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.IIndexPredicate;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleIterator;
import se.kuseman.payloadbuilder.api.session.IQuerySession;
import se.kuseman.payloadbuilder.core.operator.BatchCacheOperator.CacheSettings;
import se.kuseman.payloadbuilder.core.operator.OperatorBuilder.BuildResult;
import se.kuseman.payloadbuilder.core.parser.Select;

/** Test of {@link OperatorBuilder} with index on tables */
public class OperatorBuilderBatchHashJoinTest extends AOperatorTest
{
    @Test
    public void test_batch_cache_asterisk_is_populated()
    {
        String query = "select a.col, b.col1 " + "from tableA a "
                       + "inner join "
                       + "("
                       + "   select c.col1"
                       + "   from tableB c"
                       + ") b with (cacheName = 'tableB', cacheKey = @param, cacheTTL = 'PT10m') "
                       + "  on b.col1 = a.col1";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("tableB", all("tableB", asList("col1")))), operators);
        session.getCatalogRegistry()
                .registerCatalog("c", c);
        session.setDefaultCatalogAlias("c");

        Select select = s(query);
        OperatorBuilder.create(session, select);
    }

    @Test
    public void test_batch_cache()
    {
        String query = "select a.col, b.col " + "from tableA a " + "inner join tableB b with (cacheName = 'tableB', cacheKey = @param, cacheTTL = 'PT10m') " + "  on b.col1 = a.col1";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("tableB", all("tableB", asList("col1")))), operators);
        session.getCatalogRegistry()
                .registerCatalog("c", c);
        session.setDefaultCatalogAlias("c");

        Select select = s(query);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(3, "INNER JOIN", operators.get(0),
                new BatchCacheOperator(2, operators.get(1), new ExpressionOrdinalValuesFactory(asList(en("b.col1"))),
                        new CacheSettings(new ExpressionFunction(en("'tableB'")), new ExpressionFunction(en("@param")), new ExpressionFunction(en("'PT10m'")))),
                new ExpressionOrdinalValuesFactory(asList(en("a.col1"))), new ExpressionHashFunction(asList(en("b.col1"))), new ExpressionPredicate(en("b.col1 = a.col1")),
                new DefaultTupleMerger(-1, 1, 2), false, false, c.getIndices(session, "", QualifiedName.of("tableB"))
                        .get(0),
                null);

        assertEquals(expected, buildResult.getOperator());
    }

    @Test
    public void test_correlated_with_index_access()
    {
        String queryString = "SELECT s.art_id " + "FROM source s "
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
        Catalog c = catalog(ofEntries(entry("article", all("article", asList("art_id")))), operators);
        session.getCatalogRegistry()
                .registerCatalog("c", c);
        session.setDefaultCatalogAlias("c");

        Select select = s(queryString);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        // TODO: the inner operator here should
        // be a cached nested loop and not a hash join
        // would yield a much more effective plan
        Operator expected = new NestedLoopJoin(5, "INNER JOIN", operators.get(0),
                new OuterValuesOperator(4,
                        new HashJoin(3, "INNER JOIN", operators.get(1), operators.get(2), new ExpressionHashFunction(asList(en("a.art_id"))), new ExpressionHashFunction(asList(en("aa.art_id"))),
                                new ExpressionPredicate(en("aa.art_id = a.art_id AND s.id = true")), new DefaultTupleMerger(-1, 3, 2), true, false),
                        new ExpressionOrdinalValuesFactory(asList(en("s.art_id")))),
                new ExpressionPredicate(en("a.art_id = s.art_id")), new DefaultTupleMerger(-1, 1, 3), true, false);

        // System.err.println(buildResult.getOperator().toString(1));
        // System.out.println(expected.toString(1));

        assertEquals(expected, buildResult.getOperator());
    }

    @Test
    public void test_nested_inner_join_with_pushdown()
    {
        String queryString = "SELECT a.art_id " + "FROM source s "
                             + "INNER JOIN "
                             + "("
                             + "  select * "
                             + "  from article a "
                             + "  INNER JOIN article_attribute aa "
                             + "    ON aa.art_id = a.art_id "
                             + "    AND aa.active_flg "
                             + ") a with (populate=true)"
                             + "  ON a.art_id = s.art_id "
                             + "  AND a.club_id = 1337 + 123 "
                             + "  AND a.country_id = 0 "
                             + "  AND a.active_flg = 1";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", all("article", asList("club_id", "country_id", "art_id"))), entry("article_attribute", all("article_attribute", asList("art_id")))), operators);
        session.getCatalogRegistry()
                .registerCatalog("c", c);
        session.setDefaultCatalogAlias("c");

        Select select = s(queryString);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(6, "INNER JOIN", operators.get(0),
                new BatchHashJoin(5, "INNER JOIN", new FilterOperator(2, operators.get(1), new ExpressionPredicate(en("a.active_flg = 1"))),
                        new FilterOperator(4, operators.get(2), new ExpressionPredicate(en("aa.active_flg = true"))), new ExpressionOrdinalValuesFactory(asList(en("a.art_id"))),
                        new ExpressionHashFunction(asList(en("aa.art_id"))), new ExpressionPredicate(en("aa.art_id = a.art_id")), new DefaultTupleMerger(-1, 3, 2), false, false,
                        c.getIndices(session, "", QualifiedName.of("article_attribute"))
                                .get(0),
                        null),
                new ExpressionOrdinalValuesFactory(asList(e("1460"), e("0"), en("s.art_id"))), new ExpressionHashFunction(asList(e("1460"), e("0"), en("a.art_id"))),
                new ExpressionPredicate(en("a.art_id = s.art_id")), new DefaultTupleMerger(-1, 1, 3), true, false, c.getIndices(session, "", QualifiedName.of("article"))
                        .get(0),
                null);

        Operator actual = buildResult.getOperator();

        // System.out.println(actual.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        TupleIterator it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_inner_join_with_pushdown()
    {
        String queryString = "SELECT a.art_id " + "FROM source s "
                             + "INNER JOIN article a "
                             + "  ON a.art_id = s.art_id "
                             + "  AND a.club_id = 1337 + 123 "
                             + "  AND a.country_id = 0 "
                             + "  AND a.active_flg = 1";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", all("article", asList("club_id", "country_id", "art_id")))), operators);
        session.getCatalogRegistry()
                .registerCatalog("c", c);
        session.setDefaultCatalogAlias("c");

        Select select = s(queryString);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(3, "INNER JOIN", operators.get(0), new FilterOperator(2, operators.get(1), new ExpressionPredicate(en("a.active_flg = 1"))),
                new ExpressionOrdinalValuesFactory(asList(e("1460"), e("0"), en("s.art_id"))), new ExpressionHashFunction(asList(e("1460"), e("0"), en("a.art_id"))),
                new ExpressionPredicate(en("a.art_id = s.art_id")), new DefaultTupleMerger(-1, 1, 2), false, false, c.getIndices(session, "", QualifiedName.of("article"))
                        .get(0),
                null);

        Operator actual = buildResult.getOperator();

        // System.out.println(actual.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        TupleIterator it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_inner_join_with_pushdown_and_wildcard_columns_index()
    {
        String queryString = "SELECT a.art_id " + "FROM source s "
                             + "INNER JOIN article a "
                             + "  ON a.art_id = s.art_id "
                             + "  AND a.club_id = 1337 + 123 "
                             + "  AND a.country_id = 0 "
                             + "  AND a.active_flg = 1 "
                             + "  AND a.art_id = 1337";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", wildcard("article"))), operators);
        session.getCatalogRegistry()
                .registerCatalog("c", c);
        session.setDefaultCatalogAlias("c");

        Select select = s(queryString);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(3, "INNER JOIN", operators.get(0),
                new FilterOperator(2, operators.get(1), new ExpressionPredicate(en("a.club_id = 1460 and a.country_id = 0 AND a.active_flg = 1 AND a.art_id = 1337"))),
                new ExpressionOrdinalValuesFactory(asList(en("s.art_id"))), new ExpressionHashFunction(asList(en("a.art_id"))), new ExpressionPredicate(en("a.art_id = s.art_id")),
                new DefaultTupleMerger(-1, 1, 2), false, false, new Index(QualifiedName.of("article"), emptyList(), ColumnsType.WILDCARD, 100), null);

        Operator actual = buildResult.getOperator();

        // System.out.println(actual.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        TupleIterator it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_inner_join_with_pushdown_and_index_on_same_column()
    {
        String queryString = "SELECT a.art_id " + "FROM source s "
                             + "INNER JOIN article a "
                             + "  ON a.art_id = s.art_id "
                             + "  AND a.club_id = 1337 + 123 "
                             + "  AND a.country_id = 0 "
                             + "  AND a.art_id = 10 ";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", all("article", asList("club_id", "country_id", "art_id")))), operators);
        session.getCatalogRegistry()
                .registerCatalog("c", c);
        session.setDefaultCatalogAlias("c");

        Select select = s(queryString);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(3, "INNER JOIN", operators.get(0), new FilterOperator(2, operators.get(1), new ExpressionPredicate(en("a.art_id = 10"))),
                new ExpressionOrdinalValuesFactory(asList(e("1460"), e("0"), en("s.art_id"))), new ExpressionHashFunction(asList(e("1460"), e("0"), en("a.art_id"))),
                new ExpressionPredicate(en("a.art_id = s.art_id")), new DefaultTupleMerger(-1, 1, 2), false, false, c.getIndices(session, "", QualifiedName.of("article"))
                        .get(0),
                null);

        Operator actual = buildResult.getOperator();

        // System.out.println(actual.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        TupleIterator it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_inner_join_with_any_index()
    {
        String queryString = "SELECT a.art_id " + "FROM source s "
                             + "INNER JOIN article a "
                             + "  ON a.art_id = s.art_id "
                             + "  AND a.club_id = s.id "
                             + "  AND a.country_id = 0 "
                             + "  AND a.art_id = 10 ";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", any("article", asList("club_id", "country_id", "art_id")))), operators);
        session.getCatalogRegistry()
                .registerCatalog("c", c);
        session.setDefaultCatalogAlias("c");

        Select select = s(queryString);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        // Verify that correct columns got picked for index predicate
        assertEquals(asList("club_id", "art_id"), ((TestOperator) operators.get(1)).indexPredicate.getIndexColumns());

        Operator expected = new BatchHashJoin(3, "INNER JOIN", operators.get(0), new FilterOperator(2, operators.get(1), new ExpressionPredicate(en("a.country_id = 0 AND a.art_id = 10"))),
                new ExpressionOrdinalValuesFactory(asList(en("s.id"), en("s.art_id"))), new ExpressionHashFunction(asList(en("a.club_id"), en("a.art_id"))),
                new ExpressionPredicate(en("a.art_id = s.art_id and a.club_id = s.id")), new DefaultTupleMerger(-1, 1, 2), false, false, c.getIndices(session, "", QualifiedName.of("article"))
                        .get(0),
                null);

        Operator actual = buildResult.getOperator();

        // System.out.println(actual.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        TupleIterator it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_inner_join_with_any_in_order_index()
    {
        String queryString = "SELECT a.art_id " + "FROM source s "
                             + "INNER JOIN article a "
                             + "  ON a.art_id = s.art_id "
                             + "  AND a.club_id = s.id "
                             + "  AND a.country_id = 0 "
                             + "  AND a.art_id = 10 ";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", anyInOrder("article", asList("art_id", "dummy", "club_id")))), operators);
        session.getCatalogRegistry()
                .registerCatalog("c", c);
        session.setDefaultCatalogAlias("c");

        Select select = s(queryString);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        // Verify that correct columns got picked for index predicate
        // Only art_id matches in order
        assertEquals(asList("art_id"), ((TestOperator) operators.get(1)).indexPredicate.getIndexColumns());

        Operator expected = new BatchHashJoin(3, "INNER JOIN", operators.get(0), new FilterOperator(2, operators.get(1), new ExpressionPredicate(en("a.country_id = 0 AND a.art_id = 10"))),
                new ExpressionOrdinalValuesFactory(asList(en("s.art_id"))), new ExpressionHashFunction(asList(en("a.art_id"))), new ExpressionPredicate(en("a.art_id = s.art_id and a.club_id = s.id")),
                new DefaultTupleMerger(-1, 1, 2), false, false, c.getIndices(session, "", QualifiedName.of("article"))
                        .get(0),
                null);

        Operator actual = buildResult.getOperator();

        // System.out.println(actual.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        TupleIterator it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_inner_join_with_any_in_order_index_with_no_first_hit()
    {
        String queryString = "SELECT a.art_id " + "FROM source s "
                             + "INNER JOIN article a "
                             + "  ON a.art_id = s.art_id "
                             + "  AND a.club_id = s.id "
                             + "  AND a.country_id = 0 "
                             + "  AND a.art_id = 10 ";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", anyInOrder("article", asList("dummy", "art_id", "club_id")))), operators);
        session.getCatalogRegistry()
                .registerCatalog("c", c);
        session.setDefaultCatalogAlias("c");

        Select select = s(queryString);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        // No index should be used here
        assertNull(((TestOperator) operators.get(1)).indexPredicate);

        // Since no index then no batch hash join
        Operator expected = new HashJoin(3, "INNER JOIN", operators.get(0), new FilterOperator(2, operators.get(1), new ExpressionPredicate(en("a.country_id = 0 AND a.art_id = 10"))),
                new ExpressionHashFunction(asList(en("s.id"), en("s.art_id"))), new ExpressionHashFunction(asList(en("a.club_id"), en("a.art_id"))),
                new ExpressionPredicate(en("a.art_id = s.art_id and a.club_id = s.id")), new DefaultTupleMerger(-1, 1, 2), false, false);

        Operator actual = buildResult.getOperator();

        // System.out.println(actual.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        TupleIterator it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_inner_join_with_populate_and_filter_and_join_pushdown()
    {
        String queryString = "SELECT a.art_id " + "FROM source s "
                             + "INNER JOIN (select * from article where internet_flg = 1) a with(populate=true)"
                             + "  ON a.art_id = s.art_id "
                             + "  AND a.club_id = 1337 + 123 "
                             + "  AND a.country_id = 0 "
                             + "  AND a.active_flg = 1";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", all("article", asList("club_id", "country_id", "art_id")))), operators);
        session.getCatalogRegistry()
                .registerCatalog("c", c);
        session.setDefaultCatalogAlias("c");

        Select select = s(queryString);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(3, "INNER JOIN", operators.get(0), new FilterOperator(2, operators.get(1), new ExpressionPredicate(en("internet_flg = 1 AND a.active_flg = 1"))),
                new ExpressionOrdinalValuesFactory(asList(e("1460"), e("0"), en("s.art_id"))), new ExpressionHashFunction(asList(e("1460"), e("0"), en("a.art_id"))),
                new ExpressionPredicate(en("a.art_id = s.art_id")), new DefaultTupleMerger(-1, 1, 2), true, false, c.getIndices(session, "", QualifiedName.of("article"))
                        .get(0),
                null);

        Operator actual = buildResult.getOperator();

        // System.out.println(actual.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        TupleIterator it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_left_join_with_populate_and_filter_doesnt_pushdown_predicate()
    {
        String queryString = "SELECT a.art_id " + "FROM source s "
                             + "LEFT JOIN "
                             + "("
                             + "  select * "
                             + "from article "
                             + "where internet_flg = 1"
                             + ") a with (populate=true)"
                             + "  ON a.art_id = s.art_id "
                             + "  AND a.club_id = 1337 + 123 "
                             + "  AND a.country_id = 0 "
                             + "  AND a.active_flg = 1"
                             + "  AND s.flag";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", all("article", asList("club_id", "country_id", "art_id")))), operators);
        session.getCatalogRegistry()
                .registerCatalog("c", c);
        session.setDefaultCatalogAlias("c");

        Select select = s(queryString);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(3, "LEFT JOIN", operators.get(0), new FilterOperator(2, operators.get(1), new ExpressionPredicate(en("internet_flg = 1 AND a.active_flg = 1"))),
                new ExpressionOrdinalValuesFactory(asList(e("1460"), e("0"), en("s.art_id"))), new ExpressionHashFunction(asList(e("1460"), e("0"), en("a.art_id"))),
                new ExpressionPredicate(en("a.art_id = s.art_id AND s.flag = true")), new DefaultTupleMerger(-1, 1, 2), true, true, c.getIndices(session, "", QualifiedName.of("article"))
                        .get(0),
                null);

        Operator actual = buildResult.getOperator();

        // System.out.println(actual.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        TupleIterator it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_left_join_pushdown_predicate()
    {
        String queryString = "SELECT a.art_id " + "FROM source s "
                             + "LEFT JOIN article a "
                             + "  ON a.art_id = s.art_id "
                             + "  AND a.club_id = 1337 + 123 "
                             + "  AND a.country_id = 0 "
                             + "  AND a.active_flg = 1";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", all("article", asList("club_id", "country_id", "art_id")))), operators);
        session.getCatalogRegistry()
                .registerCatalog("c", c);
        session.setDefaultCatalogAlias("c");

        Select select = s(queryString);
        BuildResult buildResult = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(3, "LEFT JOIN", operators.get(0), new FilterOperator(2, operators.get(1), new ExpressionPredicate(en("a.active_flg = 1"))),
                new ExpressionOrdinalValuesFactory(asList(e("1460"), e("0"), en("s.art_id"))), new ExpressionHashFunction(asList(e("1460"), e("0"), en("a.art_id"))),
                new ExpressionPredicate(en("a.art_id = s.art_id")), new DefaultTupleMerger(-1, 1, 2), false, true, c.getIndices(session, "", QualifiedName.of("article"))
                        .get(0),
                null);

        Operator actual = buildResult.getOperator();

        // System.out.println(actual.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        TupleIterator it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    private Index all(String table, List<String> columns)
    {
        return new Index(QualifiedName.of(table), columns, Index.ColumnsType.ALL, 100);
    }

    private Index wildcard(String table)
    {
        return new Index(QualifiedName.of(table), emptyList(), Index.ColumnsType.WILDCARD, 100);
    }

    private Index any(String table, List<String> columns)
    {
        return new Index(QualifiedName.of(table), columns, Index.ColumnsType.ANY, 100);
    }

    private Index anyInOrder(String table, List<String> columns)
    {
        return new Index(QualifiedName.of(table), columns, Index.ColumnsType.ANY_IN_ORDER, 100);
    }

    // static final List<String> WILDCARD_COLUMNS = asList();
    private Catalog catalog(Map<String, Index> indexByTable, List<Operator> operators)
    {
        return new Catalog("TEST")
        {
            @Override
            public List<Index> getIndices(IQuerySession session, String catalogAlias, QualifiedName table)
            {
                Index index = indexByTable.get(table.toDotDelimited());
                return index != null ? asList(index)
                        : emptyList();
            }

            @Override
            public Operator getScanOperator(OperatorData data)
            {
                Operator op = new TestOperator("scan " + data.getTableAlias()
                        .getTable()
                        .toString(), null);
                operators.add(op);
                return op;
            }

            @Override
            public Operator getIndexOperator(OperatorData data, IIndexPredicate indexPredicate)
            {
                Operator op = new TestOperator("index " + data.getTableAlias()
                        .getTable()
                        .toString(), indexPredicate);
                operators.add(op);
                return op;
            }
        };
    }

    /** Test operator */
    private static class TestOperator implements Operator
    {
        private final String name;
        private final IIndexPredicate indexPredicate;

        TestOperator(String name, IIndexPredicate indexPredicate)
        {
            this.name = name;
            this.indexPredicate = indexPredicate;
        }

        @Override
        public int getNodeId()
        {
            return 0;
        }

        @Override
        public TupleIterator open(IExecutionContext context)
        {
            if (indexPredicate != null)
            {
                assertNotNull(((StatementContext) context.getStatementContext()).getOuterOrdinalValues());
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
