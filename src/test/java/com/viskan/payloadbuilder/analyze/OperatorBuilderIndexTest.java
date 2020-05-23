package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.operator.BatchOperator.Reader;
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
public class OperatorBuilderIndexTest extends AOperatorBuilderTest
{
    @Test
    public void test()
    {
        String queryString = "SELECT a.art_id " +
            "FROM source s " +
            "INNER JOIN article a " +
            "  ON a.art_id = s.art_id ";

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
        List<Reader> seeks = new ArrayList<>();
        Catalog c = catalog(ofEntries(entry("article", asList("art_id")), entry("source", asList("art_id"))), scans, seeks);
        catalogRegistry.setDefaultCatalog(c);

        Query query = parser.parseQuery(catalogRegistry, queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(catalogRegistry, query);

        System.out.println(pair.getKey().toString(1));
        
        
//        assertEquals("source", scans.get(0).toString());
//        assertEquals("article", seeks.get(0).toString());
        
    }

    private Catalog catalog(Map<String, List<String>> keysByTable, List<Operator> scans, List<Reader> indexReaders)
    {
        return new Catalog("TEST")
        {
            @Override
            public List<Index> getIndices(QualifiedName table)
            {
                List<String> keys = keysByTable.get(table.toString());
                return keys != null ? asList(new Index(keys)) : emptyList();
            }

            @Override
            public Operator getScanOperator(TableAlias alias)
            {
                Operator op = op("scan " + alias.getTable().toString());
                scans.add(op);
                return op;
            }

            @Override
            public Reader getBatchReader(QualifiedName table, Index index)
            {
                Reader re = reader("reader " + table);
                indexReaders.add(re);
                return re;
            }
            
//            @Override
//            public Operator getIndexOperator(TableAlias alias)
//            {
//                Operator op = op("seek " + alias.getTable().toString());
//                indexSeeks.add(op);
//                return op;
//            }

            private Reader reader(final String name)
            {
                return new Reader()
                {
                    @Override
                    public Iterator<Row> open(OperatorContext context, TableAlias alias, Iterator<Object[]> outerValues)
                    {
                        return emptyIterator();
                    }
                    
                    @Override
                    public String toString()
                    {
                        return name;
                    }
                };
            }
            
            private Operator op(final String name)
            {
                return new Operator()
                {
                    @Override
                    public Iterator<Row> open(OperatorContext context)
                    {
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
