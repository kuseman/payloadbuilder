package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.QualifiedName;
import com.viskan.payloadbuilder.parser.Select;
import com.viskan.payloadbuilder.parser.TableOption;

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
public class OperatorBuilderBatchHashJoinTest extends AOperatorTest
{
    @Test
    public void test_index_access_on_from()
    {
        String queryString = "SELECT a.art_if FROM article a WHERE 10 = a.art_id AND a.active_flg";
        List<Operator> operators = new ArrayList<>();
        Catalog c = catalog(ofEntries(
                entry("article", asList("art_id"))
                ), operators);
        session.setDefaultCatalog(c);
        
        Select select = parser.parseSelect(queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(session, select);
        
        Operator expected = new FilterOperator(
                2,
                new OuterValuesOperator(1, operators.get(0), asList(e("10"))),
                new ExpressionPredicate(e("a.active_flg")));

//        System.err.println(pair.getKey().toString(1));
//        System.out.println(expected.toString(1));
        
        assertEquals(expected, pair.getKey());
        
        /*
         *  OuterValuesOperator
         *    indexScan ( 
         * 
         * 
         */
        
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
        session.setDefaultCatalog(c);

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
                        new FilterOperator(4, operators.get(2), new ExpressionPredicate(e("aa.active_flg"))),
                        new ExpressionValuesExtractor(asList(e("s.art_id"))),
                        new ExpressionValuesExtractor(asList(e("aa.art_id"))),
                        new ExpressionPredicate(e("aa.art_id = s.art_id")),
                        DefaultRowMerger.DEFAULT,
                        false,
                        false,
                        c.getIndices(QualifiedName.of("article_attribute")).get(0)),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("s.art_id"))),
                new ExpressionValuesExtractor(asList(e("1460"), e("0"), e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id")),
                DefaultRowMerger.DEFAULT,
                true,
                false,
                c.getIndices(QualifiedName.of("article")).get(0));
                
        Operator actual = pair.getKey();
        
//        System.out.println(actual.toString(1));
//        System.err.println(expected.toString(1));

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
        session.setDefaultCatalog(c);

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
                c.getIndices(QualifiedName.of("article")).get(0));
                
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
        session.setDefaultCatalog(c);

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
                c.getIndices(QualifiedName.of("article")).get(0));
                
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
        session.setDefaultCatalog(c);

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
                c.getIndices(QualifiedName.of("article")).get(0));
                
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
        session.setDefaultCatalog(c);

        Select select = parser.parseSelect(queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(session, select );

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
                c.getIndices(QualifiedName.of("article")).get(0));
                
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
            public List<Index> getIndices(QualifiedName table)
            {
                List<String> keys = keysByTable.get(table.toString());
                return keys != null ? asList(new Index(table, keys, 100)) : emptyList();
            }

            @Override
            public Operator getScanOperator(int nodeId, String catalogAlias, TableAlias alias, List<TableOption> tableOptions)
            {
                Operator op = op("scan " + alias.getTable().toString());
                operators.add(op);
                return op;
            }

            @Override
            public Operator getIndexOperator(int nodeId, String catalogAlias, TableAlias alias, Index index, List<TableOption> tableOptions)
            {
                Operator op = op("index " + alias.getTable().toString());
                operators.add(op);
                return op;
            }
            
            private Operator op(final String name)
            {
                return new Operator()
                {
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
                };
            }
        };
    }
}
