package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;
import com.viskan.payloadbuilder.utils.IteratorUtils;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.IntStream;

import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.iterators.ObjectGraphIterator;
import org.junit.Test;

import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

public class PopulatingJoinOperatorTest
{
    @Test
    public void test1()
    {
        //        RoaringBitmap rb = new RoaringBitmap();
        //        IntStream.range(0, 10000000).forEach(i -> rb.add(i));

        for (int i = 0; i < 80; i++)
        {
            System.out.println(i % 20);
        }

        //        TIntHashSet set = new TIntHashSet();
        //        IntStream.range(0, 10000000).forEach(i -> set.add(i));
        //
        //        System.out.println(FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
    }

    @Test
    public void test()
    {
        TableAlias source = TableAlias.of(null, QualifiedName.of("source"), "s");
        TableAlias article = TableAlias.of(source, QualifiedName.of("article"), "a");
        TableAlias articleAttribute = TableAlias.of(source, QualifiedName.of("articleAttribute"), "aa");
        TableAlias articlePrice = TableAlias.of(articleAttribute, QualifiedName.of("articlePrice"), "ap");
        TableAlias attribute1 = TableAlias.of(articleAttribute, QualifiedName.of("attribute1"), "a1");
        TableAlias articleAttributeMedia = TableAlias.of(source, QualifiedName.of("articleAttributeMedia"), "aam");

        Random rnd = new Random();

        List<Row> sRows = IntStream.range(0, 10).mapToObj(i -> Row.of(source, i, new Object[] {i})).collect(toList());
        List<Row> aRows = IntStream.range(0, 10).mapToObj(i -> Row.of(article, 1000 + i, new Object[] {i})).collect(toList());
        List<Row> aaRows = IntStream.range(0, 20).mapToObj(i -> Row.of(articleAttribute, 2000 + i, new Object[] {rnd.nextInt(10)})).collect(toList());
        List<Row> apRows = IntStream.range(0, 80).mapToObj(i -> Row.of(articlePrice, 3000 + i, new Object[] {rnd.nextInt(50)})).collect(toList());
        List<Row> a1Rows = IntStream.range(0, 80).mapToObj(i -> Row.of(attribute1, 4000 + i, new Object[] {rnd.nextInt(20)})).collect(toList());
        List<Row> aamRows = IntStream.range(0, 10).mapToObj(i -> Row.of(articleAttributeMedia, 5000 + i, new Object[] {rnd.nextInt(10)})).collect(toList());

        OperatorContext ctx = new OperatorContext();
        //
        /*
         * 1. join s with a
         * 2. join ((s, a) with aa)
         * 3. join (aa with ap)
         * 4. join ((s, a, aa) with (aa, ap))
        */

        //        Operator res = new NestedLoop(
        //                new Distinct(
        //                        new ParentRowOperator(
        //                                new NestedLoop(
        //                                        new NestedLoop(
        //                                                c -> IteratorUtils.getChildRowsIterator(
        //                                                        new NestedLoop(
        //                                                                new NestedLoop(
        //                                                                        (cc) -> sRows.iterator(),
        //                                                                        new CachingOperator(new FetchWithParents(null, aRows)),
        //                                                                        row -> Objects.equals(row.getObject(0), row.getParents().get(0).getObject(0)),
        //                                                                        RowMerger.DEFAULT,
        //                                                                        true),
        //                                                                new CachingOperator(new FetchWithParents(null, aaRows)),
        //                                                                row -> Objects.equals(row.getObject(0), row.getParents().get(0).getObject(0)),
        //                                                                RowMerger.DEFAULT,
        //                                                                true).open(c),
        //                                                        1).iterator(),
        //                                                new CachingOperator(new FetchWithParents(null, apRows)),
        //                                                row -> Objects.equals(row.getObject(0), row.getParents().get(0).getObject(0)),
        //                                                RowMerger.DEFAULT,
        //                                                true),
        //                                        new CachingOperator(new FetchWithParents(null, a1Rows)),
        //                                        row -> Objects.equals(row.getObject(0), row.getParents().get(0).getObject(0)),
        //                                        RowMerger.DEFAULT,
        //                                        true))),
        //                new CachingOperator(new FetchWithParents(null, aamRows)),
        //                row -> Objects.equals(row.getObject(0), row.getParents().get(0).getObject(0)),
        //                RowMerger.DEFAULT,
        //                true);

        Operator s = (c) -> sRows.iterator();

        // start of source

        Operator s_a = new RowSpool("parents", article, false, s, 
                new NestedLoop(
                        new SpoolScan("parents"),
                        new CachingOperator(new FetchWithSpool(aRows, "parents")),
                        row -> Objects.equals(row.getObject(0), row.getParents().get(0).getObject(0)),
                        true));

        Operator s_a_aa = new RowSpool("parents", articleAttribute, false, s_a, 
                new NestedLoop(
                        new SpoolScan("parents"),
                        new CachingOperator(new FetchWithSpool(aaRows, "parents")),
                        row -> Objects.equals(row.getObject(0), row.getParents().get(0).getObject(0)),
                        true));

        // articleAttribute children
        
        Operator s_a_aa_children = c -> IteratorUtils.getChildRowsIterator(s_a_aa.open(c), 1).iterator();

        Operator aa_ap = new RowSpool("parents", articleAttribute, true, s_a_aa_children,
                new NestedLoop(
                        new SpoolScan("parents"),
                        new CachingOperator(new FetchWithSpool(apRows, "parents")),
                        row -> Objects.equals(row.getObject(0), row.getParents().get(0).getObject(0)),
                        false));

        Operator aa_a1 = new RowSpool("parents", articleAttribute, true, aa_ap,
                new NestedLoop(
                        new SpoolScan("parents"),
                        new CachingOperator(new FetchWithSpool(a1Rows, "parents")),
                        row -> Objects.equals(row.getObject(0), row.getParents().get(0).getObject(0)),
                        false));

        // When last join in populating level is complete
        // stream distinct parent rows to return to correct level
        Operator s_a_aa_ap = new Distinct(new ParentRowOperator(aa_a1));

        // end articleAttribute

        Operator s_a_aa_aam = new RowSpool("parents", articleAttributeMedia, false, s_a_aa_ap,
                new NestedLoop(
                        new SpoolScan("parents"),
                        new CachingOperator(new FetchWithSpool(aamRows, "parents")),
                        row -> Objects.equals(row.getObject(0), row.getParents().get(0).getObject(0)),
                        true));

        // end of source

        int count = 0;
        Iterator<Row> rit = s_a_aa_aam.open(ctx);// new PopulatingJoinOperator(s, s_a_aa_children, "top").open(ctx);
        while (rit.hasNext())
        {
            Row r = rit.next();
            System.out.println(r);
            count++;
        }

        System.out.println("Row count: " + count);
        
        
        /*
         * A tuple is equal if all non populating positions are equal
         * 
         * Tuples
         * 1 s(0) (a(0) aa(0) aam(0))
         * 2 s(0) (a(0) aa(1) aam(1))
         * 3 s(0) (a(0) aa(2) aam(2))
         * 4 s(0) (a(0) aa(0) aam(0))
         * 5 s(0) (a(0) aa(1) aam(1))
         * 6 s(0) (a(0) aa(2) aam(2))
         * 7 s(0) (a(0) aa(0) aam(0))
         * 8 s(0) (a(0) aa(1) aam(1))
         * 9 s(0) (a(0) aa(2) aam(2))
         * 
         * All non populating => tuples as thy come
         * 
         * All populating but aa
         * 
         * 1. Tuple n populates all populating rows
         * 2. If Tuple n+1 are equal to tuple n 
         * 
         * 
         */
        
        
    }
    
    /** Scan rows from spool with provided key */
    static class SpoolScan implements Operator
    {
        private final String key;

        SpoolScan(String key)
        {
            this.key = key;
        }

        @Override
        public Iterator<Row> open(OperatorContext context)
        {
            System.out.println("Scanning spool " + key);
            return context.getSpoolRows(key).iterator();
        }
    }

    /**
     * Extracts rows from operator and puts them in spool context
     * for reuse before calling child.
     * Is used when operator rows are needed multiple times
     * later on in plan.
     **/
    static class RowSpool implements Operator
    {
        private final String key;
        /** Alias to clear child rows for. */
        private final TableAlias alias;
        /** Should child rows for {@link #alias} be cleared when spooling
         * rows. Is used when a NON populating join is coming as child operator
         * because then new child rows will be generated and old ones cleared. */
        private final boolean clearChildRows;
        private final Operator operator;
        private final Operator child;

        RowSpool(String key, TableAlias alias, boolean clearChildRows, Operator operator, Operator child)
        {
            this.key = requireNonNull(key, "Key");
            this.alias = requireNonNull(alias, "alias");
            this.clearChildRows = clearChildRows;
            this.operator = requireNonNull(operator, "operator");
            this.child = requireNonNull(child, "child");
        }

        @Override
        public Iterator<Row> open(OperatorContext context)
        {
            System.out.println("Spooling " + key);
            
            List<Row> rows = new ArrayList<>();
            Iterator<Row> it = operator.open(context);
            while (it.hasNext())
            {
                Row row = it.next();
                if (clearChildRows)
                {
                    // If the operator rows are of the same type
                    // as the alias that should be cleared, move to parents
                    // instead, this means that is't a nested table alias
                    if (row.getTableAlias() == alias)
                    {
                        for (Row parent : row.getParents())
                        {
                            List<Row> childRows = parent.getChildRows(alias.getParentIndex());
                            if (childRows != null)
                            {
                                childRows.clear();
                            }
                        }
                    }
                    else
                    {
                        List<Row> childRows = row.getChildRows(alias.getParentIndex());
                        if (childRows != null)
                        {
                            childRows.clear();
                        }
                    }
                }
                rows.add(row);
            }

            context.storeSpoolRows(key, rows);
            return child.open(context);
        }
    }

    /** Streams parent rows from operators rows */
    static class ParentRowOperator implements Operator
    {
        private final Operator op;

        ParentRowOperator(Operator op)
        {
            this.op = op;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Iterator<Row> open(OperatorContext context)
        {
            final Iterator<Row> it = op.open(context);
            return new ObjectGraphIterator(it, new Transformer()
            {
                Iterator<Row> parentIt;

                @Override
                public Object transform(Object input)
                {
                    Row row = (Row) input;
                    if (parentIt == null)
                    {
                        parentIt = row.getParents().iterator();
                        return parentIt;
                    }
                    else if (!parentIt.hasNext())
                    {
                        parentIt = null;
                    }
                    return row;
                }
            });
        }
    }

    static class CachingOperator implements Operator
    {
        Operator target;
        List<Row> rows = null;

        CachingOperator(Operator target)
        {
            this.target = target;
        }

        @Override
        public Iterator<Row> open(OperatorContext context)
        {
            if (rows == null)
            {
                rows = new ArrayList<>();
                Iterator<Row> it = target.open(context);
                while (it.hasNext())
                {
                    rows.add(it.next());
                }
            }
            return rows.iterator();
        }
    }

    static class FetchWithSpool implements Operator
    {
        List<Row> target;       // Fetch from ES
        private final String spoolKey;

        FetchWithSpool(List<Row> target, String spoolKey)
        {
            this.target = target;
            this.spoolKey = spoolKey;
        }

        @Override
        public Iterator<Row> open(OperatorContext context)
        {
            List<Row> spoolRows = context.getSpoolRows(spoolKey);
            if (!spoolRows.isEmpty())
            {
                QualifiedName parentTable = spoolRows.get(0).getTableAlias().getTable();
                QualifiedName childTable = target.get(0).getTableAlias().getTable();

                System.out.println("Fetching " + spoolRows.size() + " ids from " + childTable + " with parents " + parentTable);
                // Clear parent rows when fetch is complete
                //                context.setParentRows(emptyList());
            }

            return target.iterator();
        }

    }

    static class PopulatingJoin implements Operator
    {
        Operator op;
        List<Row> distinctParents;

        PopulatingJoin(Operator op)
        {
            this.op = op;
        }

        @Override
        public Iterator<Row> open(OperatorContext context)
        {
            if (distinctParents == null)
            {
                distinctParents = new ArrayList<>();
                TIntSet hash = new TIntHashSet();
                Iterator<Row> it = op.open(context);
                while (it.hasNext())
                {
                    Row row = it.next();
                    if (hash.add(row.getPos()))
                    {
                        distinctParents.add(row);
                    }
                }
            }

            return distinctParents.iterator();
        }
    }
}
