package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.operator.BatchHashJoin;
import com.viskan.payloadbuilder.operator.DefaultRowMerger;
import com.viskan.payloadbuilder.operator.ExpressionPredicate;
import com.viskan.payloadbuilder.operator.ExpressionValuesExtractor;
import com.viskan.payloadbuilder.operator.FilterOperator;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.operator.Projection;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;
import com.viskan.payloadbuilder.parser.tree.Query;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyIterator;
import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

/** Test of {@link OperatorBuilder} with index on tables */
public class OperatorBuilderBatchHashJoinTest extends AOperatorBuilderTest
{
    @Test
    public void test_nested_inner_join_with_pushdown()
    {
        String queryString = "SELECT a.art_id " +
                "FROM source s " +
                "INNER JOIN " +
                "[" +
                "  article a " +
                "  INNER JOIN article_attribute aa " +
                "    ON aa.art_id = s.art_id " +
                "    AND aa.active_flg " +
                "] a " +
                "  ON a.art_id = s.art_id " +
                "  AND a.club_id = 1337 + 123 " +
                "  AND a.country_id = 0 " +
                "  AND a.active_flg = 1";
        
        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(
                entry("article", asList("club_id", "country_id", "art_id")),
                entry("article_attribute", asList("art_id"))
                ), operators);
        catalogRegistry.setDefaultCatalog(c);

        Query query = parser.parseQuery(catalogRegistry, queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(catalogRegistry, query);

        Operator expected = new BatchHashJoin(
                "INNER JOIN",
                operators.get(0),
                new BatchHashJoin(
                        "INNER JOIN",
                        new FilterOperator(operators.get(1), new ExpressionPredicate(e("a.active_flg = 1"))),
                        new FilterOperator(operators.get(2), new ExpressionPredicate(e("aa.active_flg"))),
                        new ExpressionValuesExtractor(asList(e("s.art_id"))),
                        new ExpressionValuesExtractor(asList(e("aa.art_id"))),
                        new ExpressionPredicate(e("aa.art_id = s.art_id")),
                        DefaultRowMerger.DEFAULT,
                        false,
                        false,
                        c.getIndices(QualifiedName.of("article_attribute")).get(0),
                        100),
//                new FilterOperator(operators.get(1), new ExpressionPredicate(e("a.active_flg = 1"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("s.art_id"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id")),
                DefaultRowMerger.DEFAULT,
                true,
                false,
                c.getIndices(QualifiedName.of("article")).get(0),
                100);
                
        Operator actual = pair.getKey();
        
//        System.out.println(actual.toString(1));
//        System.err.println(expected.toString(1));

        assertEquals(expected, actual);
        
        Iterator<Row> it = actual.open(new OperatorContext());
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
        catalogRegistry.setDefaultCatalog(c);

        Query query = parser.parseQuery(catalogRegistry, queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(catalogRegistry, query);

        Operator expected = new BatchHashJoin(
                "INNER JOIN",
                operators.get(0),
                new FilterOperator(operators.get(1), new ExpressionPredicate(e("a.active_flg = 1"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("s.art_id"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id")),
                DefaultRowMerger.DEFAULT,
                false,
                false,
                c.getIndices(QualifiedName.of("article")).get(0),
                100);
                
        Operator actual = pair.getKey();
        
//        System.out.println(actual.toString(1));
//        System.err.println(expected.toString(1));

        assertEquals(expected, actual);
        
        Iterator<Row> it = actual.open(new OperatorContext());
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
        catalogRegistry.setDefaultCatalog(c);

        Query query = parser.parseQuery(catalogRegistry, queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(catalogRegistry, query);

        Operator expected = new BatchHashJoin(
                "INNER JOIN",
                operators.get(0),
                new FilterOperator(operators.get(1), new ExpressionPredicate(e("a.active_flg = 1 AND internet_flg = 1"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("s.art_id"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id")),
                DefaultRowMerger.DEFAULT,
                true,
                false,
                c.getIndices(QualifiedName.of("article")).get(0),
                100);
                
        Operator actual = pair.getKey();
        
//        System.out.println(actual.toString(1));
//        System.err.println(expected.toString(1));

        assertEquals(expected, actual);
        
        Iterator<Row> it = actual.open(new OperatorContext());
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
        catalogRegistry.setDefaultCatalog(c);

        Query query = parser.parseQuery(catalogRegistry, queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(catalogRegistry, query);

        Operator expected = new BatchHashJoin(
                "LEFT JOIN",
                operators.get(0),
                new FilterOperator(operators.get(1), new ExpressionPredicate(e("internet_flg = 1"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("s.art_id"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id AND a.active_flg = 1")),
                DefaultRowMerger.DEFAULT,
                true,
                true,
                c.getIndices(QualifiedName.of("article")).get(0),
                100);
                
        Operator actual = pair.getKey();
        
//        System.out.println(actual.toString(1));
//        System.err.println(expected.toString(1));

        assertEquals(expected, actual);
        
        Iterator<Row> it = actual.open(new OperatorContext());
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
        catalogRegistry.setDefaultCatalog(c);

        Query query = parser.parseQuery(catalogRegistry, queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(catalogRegistry, query);

        Operator expected = new BatchHashJoin(
                "LEFT JOIN",
                operators.get(0),
                operators.get(1),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("s.art_id"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id AND a.active_flg = 1")),
                DefaultRowMerger.DEFAULT,
                false,
                true,
                c.getIndices(QualifiedName.of("article")).get(0),
                100);
                
        Operator actual = pair.getKey();
        
//        System.out.println(actual.toString(1));
//        System.err.println(expected.toString(1));

        assertEquals(expected, actual);
        
        Iterator<Row> it = actual.open(new OperatorContext());
        assertFalse(it.hasNext());
    }
    
    @Test
    public void test()
    {
        String queryString = "SELECT a.art_id " +
            "FROM source s " +
            "INNER JOIN article a " +
            "  ON a.art_id = s.art_id " +
            "  AND a.id = s.id + a.id2 " +
            "  AND a.active_flg ";

        /*
         * from source s
         * inner join article a
         *   on a.art_id = s.art_id
         * 
         * 
         * BatchedOperator
         * 
         * - read rows from s, push down and read a, join
         *
         * 
         * 
         */
        
        
        /*
         * Outer index only
         *
         * - N/A No optimization can be made since there are no rows to
         *       push downstream
         *
         *  - outer.getOperator
         *  - inner.getOperator
         *  - HashMatch
         *
         * Inner index only
         *
         * - outer.getOperator
         * - inner.getIndexOperator
         * - BatchedHashMatch join
         *
         * Both index
         *
         *  - outer.getOperator
         *  - inner.getIndexOperator
         *  - if index shares keys
         *      BatchedMergeJoin
         *    else
         *      BatchedHashMatch
         *
         * No index
         *
         *  - outer.getOperator
         *  - inner.getOperator
         *  - HashMatch
         *
         */

        List<Operator> scans = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", asList("art_id"))), scans);
        catalogRegistry.setDefaultCatalog(c);

        Query query = parser.parseQuery(catalogRegistry, queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(catalogRegistry, query);

        Operator op = pair.getKey();
        Iterator<Row> it = op.open(new OperatorContext());
        it.hasNext();
        
        System.out.println(op.toString(1));
        
        
//        assertEquals("source", scans.get(0).toString());
//        assertEquals("article", seeks.get(0).toString());
        
    }
    
    private Catalog catalog(
            Map<String, List<String>> keysByTable,
            List<Operator> operators)
    {
        return new Catalog("TEST")
        {
            @Override
            public List<Index> getIndices(QualifiedName table)
            {
                List<String> keys = keysByTable.get(table.toString());
                return keys != null ? asList(new Index(table, keys)) : emptyList();
            }

            @Override
            public Operator getOperator(TableAlias alias)
            {
                Operator op = op("scan " + alias.getTable().toString());
                operators.add(op);
                return op;
            }

            private Operator op(final String name)
            {
                return new Operator()
                {
                    @Override
                    public Iterator<Row> open(OperatorContext context)
                    {
                        if (keysByTable.containsKey(name))
                        {
                            assertNotNull(context.getIndex());
                            assertNotNull(context.getOuterIndexValues());
                        }
                        return emptyIterator();
                    }

                    @Override
                    public String toString()
                    {
                        return name;
                    }
                };
            }
        };
    }
}
