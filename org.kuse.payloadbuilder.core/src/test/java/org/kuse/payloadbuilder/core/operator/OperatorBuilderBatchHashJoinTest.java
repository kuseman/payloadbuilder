package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.Select;

/** Test of {@link OperatorBuilder} with index on tables */
public class OperatorBuilderBatchHashJoinTest extends AOperatorTest
{
    @Test
    public void test_index_access_on_from()
    {
        String queryString = "SELECT a.art_if FROM article a WHERE 10 = a.art_id AND a.active_flg";
        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(
                entry("article", asList("art_id"))), operators);
        session.getCatalogRegistry().registerCatalog("c", c);
        session.setDefaultCatalog("c");

        Select select = parser.parseSelect(queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(session, select);

        assertTrue("Operator should index access", ((TestOperator) operators.get(0)).index);
        
        Operator expected = new OuterValuesOperator(
                2,
                new FilterOperator(
                        1,
                        operators.get(0),
                        new ExpressionPredicate(e("a.active_flg = true"))),
                asList(e("10")));

//        System.err.println(pair.getKey().toString(1));
//        System.err.println();
//        System.out.println(expected.toString(1));

        assertEquals(expected, pair.getKey());
    }
    
    @Test
    public void test_correlated_with_index_access()
    {
        String queryString = "SELECT s.art_id "
            + "FROM source s "
            + "INNER JOIN "
            + "["
            + "  article a"
            + "  INNER JOIN [articleAttribute] aa"
            + "    ON aa.art_id = a.art_id "
            + "    AND s.id "
            + "] a"
            + "  ON a.art_id = s.art_id";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(
                entry("article", asList("art_id"))), operators);
        session.getCatalogRegistry().registerCatalog("c", c);
        session.setDefaultCatalog("c");
        
        Select select = parser.parseSelect(queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(session, select);
        
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
                                DefaultRowMerger.DEFAULT,
                                true,
                                false),
                        asList(e("s.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id")),
                DefaultRowMerger.DEFAULT,
                true,
                false);
        
//        System.out.println(pair.getKey().toString(1));
//        System.out.println(expected.toString(1));

        assertEquals(expected, pair.getKey());
    }

    @Test
    public void test_nested_inner_join_with_pushdown()
    {
        String queryString = "SELECT a.art_id " +
            "FROM source s " +
            "INNER JOIN " +
            "[" +
            "  article a " +
            "  INNER JOIN article_attribute aa " +
            "    ON aa.art_id = a.art_id " +
            "    AND aa.active_flg " +
            "] a " +
            "  ON a.art_id = s.art_id " +
            "  AND a.club_id = 1337 + 123 " +
            "  AND a.country_id = 0 " +
            "  AND a.active_flg = 1";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(
                entry("article", asList("club_id", "country_id", "art_id")),
                entry("article_attribute", asList("art_id"))), operators);
        session.getCatalogRegistry().registerCatalog("c", c);
        session.setDefaultCatalog("c");

        Select select = parser.parseSelect(queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(
                6,
                "INNER JOIN",
                operators.get(0),
                new BatchHashJoin(
                        5,
                        "INNER JOIN",
                        new FilterOperator(2, operators.get(1), new ExpressionPredicate(e("a.active_flg = 1"))),
                        new FilterOperator(4, operators.get(2), new ExpressionPredicate(e("aa.active_flg = true"))),
                        new ExpressionValuesExtractor(asList(e("a.art_id"))),
                        new ExpressionValuesExtractor(asList(e("aa.art_id"))),
                        new ExpressionPredicate(e("aa.art_id = a.art_id")),
                        DefaultRowMerger.DEFAULT,
                        false,
                        false,
                        c.getIndices(session, "", QualifiedName.of("article_attribute")).get(0),
                        null),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("s.art_id"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id")),
                DefaultRowMerger.DEFAULT,
                true,
                false,
                c.getIndices(session, "", QualifiedName.of("article")).get(0),
                null);

        Operator actual = pair.getKey();

//                System.out.println(actual.toString(1));
//                System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        Iterator<Row> it = actual.open(new ExecutionContext(session));
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
        session.setDefaultCatalog("c");

        Select select = parser.parseSelect(queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(
                3,
                "INNER JOIN",
                operators.get(0),
                new FilterOperator(2, operators.get(1), new ExpressionPredicate(e("a.active_flg = 1"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("s.art_id"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id")),
                DefaultRowMerger.DEFAULT,
                false,
                false,
                c.getIndices(session, "", QualifiedName.of("article")).get(0),
                null);

        Operator actual = pair.getKey();

        //        System.out.println(actual.toString(1));
        //        System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        Iterator<Row> it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_inner_join_with_populate_and_filter_and_join_pushdown()
    {
        String queryString = "SELECT a.art_id " +
            "FROM source s " +
            "INNER JOIN [article where internet_flg = 1] a " +
            "  ON a.art_id = s.art_id " +
            "  AND a.club_id = 1337 + 123 " +
            "  AND a.country_id = 0 " +
            "  AND a.active_flg = 1";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", asList("club_id", "country_id", "art_id"))), operators);
        session.getCatalogRegistry().registerCatalog("c", c);
        session.setDefaultCatalog("c");

        Select select = parser.parseSelect(queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(
                3,
                "INNER JOIN",
                operators.get(0),
                new FilterOperator(2, operators.get(1), new ExpressionPredicate(e("a.active_flg = 1 AND internet_flg = 1"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("s.art_id"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id")),
                DefaultRowMerger.DEFAULT,
                true,
                false,
                c.getIndices(session, "", QualifiedName.of("article")).get(0),
                null);

        Operator actual = pair.getKey();

        //        System.out.println(actual.toString(1));
        //        System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        Iterator<Row> it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_left_join_with_populate_and_filter_doesnt_pushdown_predicate()
    {
        String queryString = "SELECT a.art_id " +
            "FROM source s " +
            "LEFT JOIN [article where internet_flg = 1] a " +
            "  ON a.art_id = s.art_id " +
            "  AND a.club_id = 1337 + 123 " +
            "  AND a.country_id = 0 " +
            "  AND a.active_flg = 1";

        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", asList("club_id", "country_id", "art_id"))), operators);
        session.getCatalogRegistry().registerCatalog("c", c);
        session.setDefaultCatalog("c");

        Select select = parser.parseSelect(queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(
                3,
                "LEFT JOIN",
                operators.get(0),
                new FilterOperator(2, operators.get(1), new ExpressionPredicate(e("internet_flg = 1"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("s.art_id"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id AND a.active_flg = 1")),
                DefaultRowMerger.DEFAULT,
                true,
                true,
                c.getIndices(session, "", QualifiedName.of("article")).get(0),
                null);

        Operator actual = pair.getKey();

        //        System.out.println(actual.toString(1));
        //        System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        Iterator<Row> it = actual.open(new ExecutionContext(session));
        assertFalse(it.hasNext());
    }

    @Test
    public void test_left_join_doesnt_pushdown_predicate()
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
        session.setDefaultCatalog("c");

        Select select = parser.parseSelect(queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(session, select);

        Operator expected = new BatchHashJoin(
                2,
                "LEFT JOIN",
                operators.get(0),
                operators.get(1),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("s.art_id"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id AND a.active_flg = 1")),
                DefaultRowMerger.DEFAULT,
                false,
                true,
                c.getIndices(session, "", QualifiedName.of("article")).get(0),
                null);

        Operator actual = pair.getKey();

        //        System.out.println(actual.toString(1));
        //        System.err.println(expected.toString(1));

        assertEquals(expected, actual);

        Iterator<Row> it = actual.open(new ExecutionContext(session));
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
                Operator op = new TestOperator("index " + data.getTableAlias().getTable().toString(), keysByTable, false);
                operators.add(op);
                return op;
            }

            @Override
            public Operator getIndexOperator(OperatorData data, Index index)
            {
                Operator op = new TestOperator("index " + data.getTableAlias().getTable().toString(), keysByTable, true);
                operators.add(op);
                return op;
            }
        };
    }
    
    private static class TestOperator implements Operator
    {
        private final String name;
        private final Map<String, List<String>> keysByTable;
        private final boolean index;

        TestOperator(String name, Map<String, List<String>> keysByTable, boolean index)
        {
            this.name = name;
            this.keysByTable = keysByTable;
            this.index = index;
        }
       
        @Override
        public int getNodeId()
        {
            return 0;
        }

        @Override
        public Iterator<Row> open(ExecutionContext context)
        {
            if (keysByTable.containsKey(name))
            {
                assertNotNull(context.getOperatorContext().getOuterIndexValues());
            }
            return emptyIterator();
        }

        @Override
        public String toString()
        {
            return name;
        }
        
    }
}