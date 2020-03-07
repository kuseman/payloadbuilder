package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.operator.OperatorTest.BinaryExpression.Operand;

import static com.viskan.payloadbuilder.operator.RowMerger.DEFAULT;
import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.iterators.FilterIterator;
import org.apache.commons.collections.iterators.ObjectGraphIterator;
import org.apache.commons.collections.iterators.TransformIterator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import gnu.trove.map.hash.THashMap;

@Ignore
public class OperatorTest extends Assert
{
    /*
     * 1. ANTLR
     *    Compose language, write tests
     * 2.
     *    SOLVED: Nested loop join with Janino compiled predicate, best bang for the buck.
     *    Operators optimizing for (campaign join)
     *    Split Table function for array-fields (includeArtIds)
     *    Sub-query ?
     *    If-optimizing (split IN-expression into UNION ALL with AND NOT for non table references)
     * 3. Expression node implementations
     * 3. Registering of schema
     *    Tables, functions
     *    Factory for creating operators
     * 4. Fetching data from source
     *     Table function ?
     * 5. Push predicate into scan operator
     *
     *
     *
     *
     *
     */

    @BeforeClass
    public static void setupClass()
    {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        List<String> arguments = runtimeMxBean.getInputArguments();
        System.out.println("Jvm args " + arguments);
    }

    public static class CachedSupplier<T> implements Supplier<T>
    {
        T value;
        private final Supplier<T> sup;

        public CachedSupplier(Supplier<T> sup)
        {
            this.sup = sup;
        }

        @Override
        public T get()
        {
            if (value == null)
            {
                value = sup.get();
            }
            return value;
        }

    }

    private Operator operator3()
    {
        Random rnd = new Random();

        TableAlias articleAttributeMeta = TableAlias.of(null, "articleAttribute", "aa");
        //      articleAttributeMeta.setParentIndex(0);
        TableAlias priceMeta = TableAlias.of(articleAttributeMeta, "price", "ap");
        priceMeta.setColumns(new String[] {"price_sales", "sku_id"});
        //      priceMeta.setparentIndex = 0;

        // articleAttribute (art_id,O sku_id, attr1_id)
        Operator articleAttributeScan = new ListScan(
                articleAttributeMeta,
                new CachedSupplier<>(() -> IntStream.range(1, 100000).mapToObj(i -> new Object[] {rnd.nextInt(20000), i, rnd.nextInt(300)}).collect(Collectors.toList())));

        // price (col, sku_id)
        Operator priceScan = new ListScan(
                priceMeta,
                new CachedSupplier<>(() -> IntStream.range(1, 500000).mapToObj(i -> new Object[] {rnd.nextFloat() * 1000, i / 5}).collect(Collectors.toList())));

        Operator aa_ap = new Distinct(new HashMatch(
                articleAttributeScan,
                priceScan,
                t -> (int) t.getObject(1),
                t -> (int) t.getObject(1),
                row -> Objects.equals(row.getParent().getObject(1), row.getObject(1)),
                RowMerger.DEFAULT));

        return new GroupBy(priceScan, row -> row.getObject(1));
    }

    private Operator operator2()
    {
        Random rnd = new Random();

        TableAlias sourceMeta = TableAlias.of(null, "source", "s");
        //      sourceMeta.columns =new String[] { "col", "art_id" };
        TableAlias articleAttributeMeta = TableAlias.of(sourceMeta, "articleAttribute", "aa");
        //      articleAttributeMeta.setParentIndex(0);
        TableAlias priceMeta = TableAlias.of(articleAttributeMeta, "price", "ap");
        //      priceMeta.setparentIndex = 0;

        /**
         * from source s inner join articleAttribute aa ( on aa.art_id == s.art_id inner join price p on p.sku_id == aa.sku_id )
         */

        // _source (col, art_id)
        Operator sourceScan = new ListScan(
                sourceMeta,
                new CachedSupplier<>(() -> IntStream.range(1, 20000).mapToObj(i -> new Object[] {i, i}).collect(Collectors.toList())));

        // articleAttribute (art_id, sku_id, attr1_id)
        Operator articleAttributeScan = new ListScan(
                articleAttributeMeta,
                new CachedSupplier<>(() -> IntStream.range(1, 100000).mapToObj(i -> new Object[] {rnd.nextInt(20000), i, rnd.nextInt(300)}).collect(Collectors.toList())));

        // price (col, sku_id)
        Operator priceScan = new ListScan(
                priceMeta,
                new CachedSupplier<>(() -> IntStream.range(1, 500000).mapToObj(i -> new Object[] {rnd.nextFloat(), i / 5}).collect(Collectors.toList())));

        // articleAttribute.sku_id == price.sku_id
        Operator aa_ap = new HashMatch(
                articleAttributeScan,
                priceScan,
                t -> (int) t.getObject(1),
                t -> (int) t.getObject(1),
                row -> Objects.equals(row.getParent().getObject(1), row.getObject(1)),
                RowMerger.COPY);

        // _source.art_id == articleAttribute.art_id
        Operator op = new Distinct(new HashMatch(
                sourceScan,
                aa_ap,
                t -> (int) t.getObject(1),
                t -> (int) t.getObject(0),
                row -> Objects.equals(row.getParent().getObject(1), row.getObject(0)),
                DEFAULT));

        return op;
    }

    private Operator operator1()
    {
        Random rnd = new Random();

        TableAlias sourceMeta = TableAlias.of(null, "source", "s");
        sourceMeta.setColumns(new String[] {"col", "art_id"});
        TableAlias articleAttributeMeta = TableAlias.of(sourceMeta, "articleAttribute", "aa");
        articleAttributeMeta.setColumns(new String[] {"art_id", "sku_id", "attr1_id", "attr2_id", "attr3_id"});
        //        articleAttributeMeta.setParentIndex(0);
        TableAlias priceMeta = TableAlias.of(articleAttributeMeta, "price", "ap");
        priceMeta.setColumns(new String[] {"price_sales", "sku_id"});
        //        priceMeta.setparentIndex = 0;
        TableAlias attribute1Meta = TableAlias.of(articleAttributeMeta, "attribute1", "a1");
        //        attribute1Meta.setParentIndex(1);
        attribute1Meta.setColumns(new String[] {"col", "attr1_id"});
        TableAlias attribute2Meta = TableAlias.of(articleAttributeMeta, "attribute2", "a2");
        //        attribute2Meta.setParentIndex(2);
        attribute2Meta.setColumns(new String[] {"col", "attr2_id"});
        TableAlias attribute3Meta = TableAlias.of(articleAttributeMeta, "attribute3", "a3");
        //        attribute3Meta.setParentIndex(3);
        attribute3Meta.setColumns(new String[] {"col", "attr3_id"});

        // _source (col, art_id)
        Operator sourceScan = new ListScan(
                sourceMeta,
                new CachedSupplier<>(() -> IntStream.range(1, 20000).mapToObj(i -> new Object[] {i, i}).collect(Collectors.toList())));

        // articleAttribute (art_id, sku_id, attr1_id, attr2_id, attr3_id)
        Operator articleAttributeScan = new ListScan(
                articleAttributeMeta,
                new CachedSupplier<>(
                        () -> IntStream.range(1, 100000).mapToObj(i -> new Object[] {rnd.nextInt(20000), i, rnd.nextInt(300), rnd.nextInt(300), rnd.nextInt(300)}).collect(Collectors.toList())));

        // price (price_sales, sku_id)
        Operator priceScan = new ListScan(
                priceMeta,
                new CachedSupplier<>(() -> IntStream.range(1, 500000).mapToObj(i -> new Object[] {rnd.nextFloat(), i / 5}).collect(Collectors.toList())));

        // attribute1 (col, attr1_id)
        Operator attribute1Scan = new ListScan(
                attribute1Meta,
                new CachedSupplier<>(() -> IntStream.range(1, 300).mapToObj(i -> new Object[] {rnd.nextGaussian(), i}).collect(Collectors.toList())));
        // attribute1 (col, attr2_id)
        Operator attribute2Scan = new ListScan(
                attribute2Meta,
                new CachedSupplier<>(() -> IntStream.range(1, 300).mapToObj(i -> new Object[] {rnd.nextGaussian(), i}).collect(Collectors.toList())));
        // attribute1 (col, attr3_id)
        Operator attribute3Scan = new ListScan(
                attribute3Meta,
                new CachedSupplier<>(() -> IntStream.range(1, 300).mapToObj(i -> new Object[] {rnd.nextGaussian(), i}).collect(Collectors.toList())));

        // articleAttribute.sku_id == price.sku_id
        Operator aa_ap = new Distinct(new HashMatch(
                articleAttributeScan,
                priceScan,
                new ColumnPathHashFunction("aa.sku_id"),
                new ColumnPathHashFunction("ap.sku_id"),
                new ColumnPathPredicate("aa.sku_id", "ap.sku_id"),
                DEFAULT));

        // articleAttribute.attr1_id == attribute1.attr1_id
        Operator aa_a1 = new Distinct(new HashMatch(
                aa_ap,
                attribute1Scan,
                new ColumnPathHashFunction("aa.attr1_id"),
                new ColumnPathHashFunction("a1.attr1_id"),
                new ColumnPathPredicate("aa.attr1_id", "a1.attr1_id"),
                DEFAULT));

        // articleAttribute.attr2_id == attribute2.attr2_id
        Operator aa_a2 = new Distinct(new HashMatch(
                aa_a1,
                attribute2Scan,
                new ColumnPathHashFunction("aa.attr2_id"),
                new ColumnPathHashFunction("a2.attr2_id"),
                new ColumnPathPredicate("aa.attr2_id", "a2.attr2_id"),
                DEFAULT));

        // articleAttribute.attr3_id == attribute3.attr3_id
        Operator aa_a3 = new Distinct(new HashMatch(
                aa_a2,
                attribute3Scan,
                new ColumnPathHashFunction("aa.attr3_id"),
                new ColumnPathHashFunction("a3.attr3_id"),
                new ColumnPathPredicate("aa.attr3_id", "a3.attr3_id"),
                DEFAULT));

        // _source.art_id == articleAttribute.art_id
        Operator op = new Distinct(new HashMatch(
                sourceScan,
                aa_a3,
                new ColumnPathHashFunction("s.art_id"),
                new ColumnPathHashFunction("aa.art_id"),
                new ColumnPathPredicate("s.art_id", "aa.art_id"),
                DEFAULT));

        return op;
    }

    @Test
    public void test_lookup()
    {
        TableAlias sourceMeta = TableAlias.of(null, "source", "s");
        sourceMeta.setColumns(new String[] {"col", "art_id"});
        TableAlias articleAttributeMeta = TableAlias.of(sourceMeta, "articleAttribute", "aa");
        articleAttributeMeta.setColumns(new String[] {"art_id", "sku_id", "attr1_id", "attr2_id", "attr3_id"});

        Operator sourceScan = new ListScan(
                sourceMeta,
                new CachedSupplier<>(() -> IntStream.range(1, 200).mapToObj(i -> new Object[] {i, i}).collect(Collectors.toList())));

        Random rnd = new Random();

        Map<Object, List<Object[]>> values = new THashMap<>(2500, 1.0f);
        IntStream.range(1, 3500000).forEach(i ->
        {
            Object[] ar = new Object[] {i % 2500 + 1, i, rnd.nextInt(300), rnd.nextInt(300), rnd.nextInt(300)};
            values.computeIfAbsent(ar[0], key -> new ArrayList<>(3500000 / 2500)).add(ar);
        });

        //        Map<Object, List<Object[]>> values = IntStream.range(1, 5000000)
        //                .mapToObj(i -> new Object[] {i / 200, i, rnd.nextInt(300), rnd.nextInt(300), rnd.nextInt(300)})
        //                .collect(Collectors.groupingBy(ar -> ar[0]));

        @SuppressWarnings("unchecked")
        Operator articleAttributeScan = context ->
        {
            MutableInt pos = new MutableInt(-1);
            List<Object> indexLookupValues = context.getIndexLookupValues("art_id");
            if (indexLookupValues != null)
            {
                System.out.println("Index lookup");
                // Index lookup
                return new ObjectGraphIterator(indexLookupValues.iterator(), t ->
                {
                    if (t instanceof List)
                    {
                        return ((List<Object>) t).iterator();
                    }
                    else if (t instanceof Object[])
                    {
                        pos.increment();
                        return Row.of(articleAttributeMeta, pos.intValue(), (Object[]) t);
                    }

                    return IteratorUtils.getIterator(values.get(t));
                });
            }
            else
            {
                System.out.println("Table scan");
                // Table scan
                return new ObjectGraphIterator(values.values().iterator(), t ->
                {
                    if (t instanceof List)
                    {
                        return ((List<Object>) t).iterator();
                    }

                    pos.increment();
                    return Row.of(articleAttributeMeta, pos.intValue(), (Object[]) t);
                });
            }
        };
        Operator join = new HashMatch(
                //                sourceScan,
                new KeyLookup(sourceScan, "art_id", row -> row.getObject(1)),
                articleAttributeScan,
                new ColumnPathHashFunction("s.art_id"),
                new ColumnPathHashFunction("aa.art_id"),
                new ColumnPathPredicate("s.art_id", "aa.art_id"),
                (outer, inner) -> outer);

        for (int i = 0; i < 100; i++)
        {
            StopWatch sw = new StopWatch();
            sw.start();
            int rowCount = 0;
            Iterator<Row> it = join.open(new OperatorContext());
            while (it.hasNext())
            {
                it.next();
                rowCount++;
            }
            sw.stop();
            System.out.println(sw.toString() + " Row count: " + rowCount + ", Memory: " + FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
        }
    }

    @Test
    public void test_tvf()
    {
        TableAlias a = TableAlias.of(null, "source", "s");
        a.setColumns(new String[] {"article_attribute"});
        Row r = Row.of(a, 0, new Object[] {
                ofEntries(entry("attribute1", ofEntries(
                        entry("buckets", asList(
                                ofEntries(entry("key", 1), entry("count", 10)),
                                ofEntries(entry("key", 2), entry("count", 20)))))))});

        /*
         * 
         * 
         * source
         *   articleAttribute (hashjoin <- sources provided)
         *   article
         *
         * source
         * articleAttribute     (hashjoin <- scan)
         *
         */
        MapExtract o = new MapExtract(new String[] {"article_attribute", "attribute1", "buckets"});

        OperatorContext ctx = new OperatorContext();
        ctx.setParentRows(asList(r));
        Iterator<Row> it = o.open(ctx);

        while (it.hasNext())
        {
            System.out.println(it.next());
        }
    }

    /** Extracts rows from parent rows.
     * Traverses a map structure and builds rows on the fly
     * */
    private static class MapExtract implements Operator
    {
        private final String[] parts;

        MapExtract(String[] parts)
        {
            this.parts = parts;
        }

        @Override
        public Iterator<Row> open(OperatorContext context)
        {
            final Iterator<Row> it = context.getParentRows().iterator();
            return new Iterator<Row>()
            {
                int pos = 0;
                Row next;
                Iterator<Map<String, Object>> mapIt;
                TableAlias parent;
                TableAlias alias = null;

                @Override
                public boolean hasNext()
                {
                    return next != null || setNext();
                }

                @Override
                public Row next()
                {
                    Row r = next;
                    next = null;
                    return r;
                }

                boolean setNext()
                {
                    while (next == null)
                    {
                        if (mapIt == null)
                        {
                            if (!it.hasNext())
                            {
                                return false;
                            }

                            Row r = it.next();

                            if (parent == null)
                            {
                                parent = r.getTableAlias();
                            }

                            // Dig down the path
                            Map<String, Object> current = (Map<String, Object>) r.getObject(parts[0]);
                            for (int i = 1; i < parts.length - 1; i++)
                            {
                                current = (Map<String, Object>) current.get(parts[i]);
                            }

                            Object last = current.get(parts[parts.length - 1]);
                            mapIt = IteratorUtils.getIterator(last);
                            continue;
                        }
                        else if (!mapIt.hasNext())
                        {
                            mapIt = null;
                            continue;
                        }

                        Map<String, Object> item = mapIt.next();

                        if (alias == null)
                        {
                            alias = TableAlias.of(parent, "mapExtract", "_mapExtract");
                            alias.setColumns(item.keySet().toArray(ArrayUtils.EMPTY_STRING_ARRAY));
                        }

                        int length = alias.getColumns().length;
                        Object[] values = new Object[length];
                        for (int i = 0; i < length; i++)
                        {
                            values[i] = item.get(alias.getColumns()[i]);
                        }

                        next = Row.of(alias, pos++, values);
                    }

                    return next != null;
                }
            };
        }

    }

    //    @Test
    //    public void test_campaign() throws CompileException, InstantiationException, IllegalAccessException
    //    {
    //        for (int i = 0; i < 100; i++)
    //        {
    //            loadCampaign();
    //        }
    //    }
    //
    //    @SuppressWarnings("unchecked")
    //    public <T> Set<T> asSet(T...items)
    //    {
    //        return new HashSet<>(asList(items));
    //    }

    //    @Test
    //    public void loadCampaign() throws CompileException, InstantiationException, IllegalAccessException
    //    {
    //        /*
    //         * from source s
    //         * inner join campaign c (
    //         *   ON (c.includeArtIds in (-1, s.art_id) OR c.includeSkuIds in (-1, s.sku_id))
    //         *   AND NOT (c.excludeArtIds in (s.art_id) OR c.excludeSkuIds in (s.sku_id))
    //         * )
    //         *
    //         * ====>
    //         *
    //         * - How to turn an array into a table?
    //         * -
    //         *
    //         * from source s
    //         * inner join campaign c (
    //         *   on
    //         *   (
    //         *     c.apply_to_articles = -1
    //         *     OR
    //         *     (
    //         *       c.includeArtIds = s.art_id
    //         *       OR c.includeSkuIds = s.sku_id
    //         *     )
    //         *   )
    //         *   AND NOT
    //         *   (
    //         *     c.excludeArtId = s.art_id
    //         *     OR c.excludeSkuIds = s.sku_id
    //         *   )
    //         * )
    //         *
    //         *
    //         */
    //
    //        TableAlias sourceMeta = TableAlias.of(null, "source", "s");
    //        sourceMeta.setColumns(new String[] {"art_id", "sku_id"});
    //        TableAlias campaignMeta = TableAlias.of(sourceMeta, "campaign", "c");
    //        campaignMeta.setColumns(new String[] {"apply_to_articles", "includeArtIds", "includeSkuIds", "excludeArtIds", "excludeSkuIds"});
    //
    //        Random rnd = new Random();
    //        // _source (art_id, sku_id)
    //        Operator sourceScan = new ListScan(
    //                sourceMeta,
    //                new CachedSupplier<>(() -> IntStream.range(1, 300000).mapToObj(i -> new Object[] {i, i / 5 + i}).collect(Collectors.toList())));
    //
    //        // articleAttribute ("apply_to_articles", "includeArtIds", "includeSkuIds", "excludeArtIds", "excludeSkuIds")
    //        Operator campaignScan = new ListScan(
    //                campaignMeta,
    //                new CachedSupplier<>(() -> IntStream.range(1, 200)
    //                        .mapToObj(i -> new Object[] {0,  asList(rnd.nextInt(300000), rnd.nextInt(300000), rnd.nextInt(300000), rnd.nextInt(300000)), asList(rnd.nextInt(1000)),
    //                                asList(rnd.nextInt(1000)), asList(rnd.nextInt(1000))})
    //                        .collect(Collectors.toList())));
    //
    //        /*
    //             (
    //                 c.apply_to_articles = 1
    //                 OR
    //                 c.includeArtIds in (s.art_id)
    //                 OR
    //                 c.includeSkuIds in (s.sku_id)
    //             )
    //             AND NOT
    //             (
    //                c.excludeArtIds in (s.art_id)
    //                OR
    //                c.excludeSkuIds in (s.sku_id)
    //             )
    //        */
    //
    //        CodeGenerator gen = new CodeGenerator();
    //        gen.generateBiPredicate(campaignMeta, expression)
    //
    ////        BiFunction<Row, Row, Object> pred = new BinaryBiExpression(
    ////                new BinaryBiExpression(
    ////                        new BinaryBiExpression(
    ////                                new ColumnPathBiExpression("apply_to_articles", 0, true),
    ////                                new ConstantBiExpression(1),
    ////                                Operand.EQ),
    ////                        new BinaryBiExpression(
    ////                                new InExpression(
    ////                                        new ColumnPathBiExpression("includeArtIds", 1, true),
    ////                                        asList(new ColumnPathBiExpression("art_id", 0, false))),
    ////                                new InExpression(
    ////                                        new ColumnPathBiExpression("includeSkuIds", 2, true),
    ////                                        asList(new ColumnPathBiExpression("sku_id", 1, false))),
    ////                                Operand.OR),
    ////                        Operand.OR),
    ////                new NotBiExpression(
    ////                        new BinaryBiExpression(
    ////                                new InExpression(
    ////                                        new ColumnPathBiExpression("excludeArtIds", 3, true),
    ////                                        asList(new ColumnPathBiExpression("art_id", 0, false))),
    ////                                new InExpression(
    ////                                        new ColumnPathBiExpression("excludeSkuIds", 4, true),
    ////                                        asList(new ColumnPathBiExpression("sku_id", 1, false))),
    ////                                Operand.OR)),
    ////                Operand.AND);
    //
    //        StopWatch sw = new StopWatch();
    //        BiFunction<Row, Row, Object> predJanino = null;
    //        for (int i = 0; i < 1; i++)
    //        {
    //            sw.reset();
    //            sw.start();
    //
    //            ClassBodyEvaluator cbe = new ClassBodyEvaluator();
    //            cbe.setDefaultImports(new String[] {"com.viskan.payloadbuilder.Row", "java.util.Objects", "java.util.Collection"});
    //            cbe.setImplementedInterfaces(new Class[] {BiFunction.class});
    //
    //            StringBuilder sb = new StringBuilder();
    //            sb.append("public Object apply(Object a, Object b) { ");
    //            sb.append("Row outer = (Row) a; Row inner = (Row) b;");
    //            sb.append("return ");
    //            ((CodeGen) pred).append(sb);
    //            sb.append(";");
    //            sb.append("}");
    //
    //            System.out.println(sb.toString());
    //            cbe.cook(sb.toString());
    //
    //            Class<?> clazz = cbe.getClazz();
    //            predJanino = (BiFunction<Row, Row, Object>) clazz.newInstance();
    //
    //            sw.stop();
    //            System.out.println("Time: " + sw.toString());
    //        }
    //
    //        BiFunction<Row, Row, Object> p = predJanino;
    //
    //        Operator op = new NestedLoop(
    //                sourceScan,
    //                campaignScan,
    //                (outer, inner) ->
    //                {
    //
    //                                        return ((Boolean) pred.apply(outer, inner)).booleanValue();
    ////                    return ((Boolean) p.apply(outer, inner)).booleanValue();
    //
    //                    //                    Object result = null;
    //                    //                    try
    //                    //                    {
    //                    //                        result = se.evaluate(new Object[] { outer, inner });
    //                    //                    }
    //                    //                    catch (InvocationTargetException e)
    //                    //                    {
    //                    //                        e.printStackTrace();
    //                    //                    }
    //
    //                    //                    boolean predRes = ((Boolean) pred.apply(outer, inner)).booleanValue();
    //
    //                    //                    if (!Objects.equals(result, predRes))
    //                    //                    {
    //                    //                        System.err.println("FEL");
    //                    //                    }
    //                    //                    return false;
    //                    //                    return ((Boolean) result).booleanValue();
    //                },
    //
    //                //                (outer, inner) -> (((Integer) inner.getObject(0)).intValue() == 1
    //                //                    ||
    //                //                    (((Collection) inner.getObject(1)).contains(outer.getObject(0))
    //                //                        ||
    //                //                        ((Collection) inner.getObject(2)).contains(outer.getObject(1))))
    //                //                    &&
    //                //                    !(((Collection) inner.getObject(3)).contains(outer.getObject(0))
    //                //                        ||
    //                //                        ((Collection) inner.getObject(4)).contains(outer.getObject(1))),
    //                RowMerger.DEFAULT);
    //
    //        for (int i = 0; i < 100; i++)
    //        {
    //            sw.reset();
    //            sw.start();
    //
    //            Iterator<Row> it = op.open(new OperatorContext());
    //            int rowCount = 0;
    //            int innerCount = 0;
    //
    //            List<Row> rows = new ArrayList<>();
    //
    //            while (it.hasNext())
    //            {
    //                Row row = it.next();
    //                rows.add(row);
    //                innerCount += row.getChildRows(0).size();
    //                rowCount++;
    //            }
    //
    //            System.out.println("Row count: " + rowCount + " Inner count: " +  innerCount + ", Memory: " + FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) + " Time: "
    //                + sw.toString());
    //
    ////            int h=0;
    ////            TIntHashSet hs = new TIntHashSet();
    ////            for (Row r : new ParentChildRowsIteraor(rows, 0))
    ////            {
    ////                if (hs.add(r.getPos()))
    ////                {
    ////                    System.out.println((++h) + " " +r);
    ////                }
    ////            }
    //
    //        }
    //    }

    @Test
    public void test1() throws JsonProcessingException
    {
        /*
         *
         * SELECT 10        field
         * ,      s.col     field1
         * ,      s.art_id  field2
         * ,
         * OBJECT
         * (
         *        13337     obj1
         * ,      ss.art_id obj2
         * ,      ss.col    obj3
         * )                field3
         * FROM source s
         * INNER JOIN source s2
         *   ON s2.id == s.id2
         */

        Operator operator = operator1();

        System.out.println(operator.toString(1));

        Map<String, Projection> projections = new LinkedHashMap<>();
        projections.put("art_id", new ColumnPathProjection("art_id"));

        projections.put("articleAttributes", new ArrayProjection(
                asList(new ObjectProjection(ofEntries(
                        entry("art_id", new ColumnPathProjection("art_id")),
                        entry("sku_id", new ColumnPathProjection("sku_id")),
                        entry("attr1_id", new ColumnPathProjection("attr1_id")),
                        entry("attr2_id", new ColumnPathProjection("attr2_id")),
                        entry("attr3_id", new ColumnPathProjection("attr3_id")),
                        entry("prices", new ArrayProjection(asList(new ObjectProjection(ofEntries(
                                entry("price_sales", new ColumnPathProjection("price_sales")),
                                entry("sku_id", new ColumnPathProjection("sku_id"))))), new AliasScan(0)))))),
                new AliasScan(0)));

        //        Map<String, Projection> objPro = new LinkedHashMap<>();
        //        objPro.put("obj1", new ConstantProjection(13337));
        //        objPro.put("obj2", new ColumnPathProjection("art_id"));
        //        objPro.put("obj3", new ColumnPathProjection("col"));
        //        objPro.put("ATTR1_ID", new ColumnPathProjection("attr1_id"));
        //        objPro.put("SKU_ID", new ColumnPathProjection("aa.sku_id"));
        //        objPro.put("PRICE_SALES", new ColumnPathProjection("aa.ap.price_sales"));
        //        objPro.put("PRICE_SALES2", new ColumnPathProjection("price_sales"));
        //        objPro.put("SUM_PRICE_SALES2", r ->
        //        {
        //            //            Object o = r.getObject(0);
        //            //            Iterable it = (Iterable) r.getObject(0);
        //            float sum = 0;
        //            //            for (Object ps : it)
        //            //            {
        //            //                sum += (float) ps;
        //            //            }
        //            return sum;
        //        });
        //
        //
        //
        //        Operator objProSelection = new Filter(
        //                new AliasScan(0),       // Scan articleAttribute
        //                r -> (int) r.getObject(2) % 2 != 0);
        //        projections.put("field3", new ObjectProjection(objPro, objProSelection));

        Query query = new Query(operator, projections);

        List<Map<String, Object>> list = emptyList();
        for (int i = 0; i < 1000; i++)
        {
            StopWatch sw = new StopWatch();
            sw.reset();
            sw.start();
            list = query.executeToMap();
            //            System.out.println(list);
            sw.stop();

            System.out.println("Row count: " + list.size() + " Memory: " + FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) + " Time: "
                + sw.toString());
        }

        // query -> outputwriter -> objectMapper -> byte[]

        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(list.get(0)));
    }

    @Test
    public void testE()
    {
        TableAlias source = TableAlias.of(null, "source", "s");
        source.setColumns(new String[] {"art_id", "idx_id"});
        TableAlias articleAttribute = TableAlias.of(source, "articleAttribute", "aa");
        articleAttribute.setColumns(new String[] {"art_id", "sku_id"});

        Row row = Row.of(source, 0, new Object[] {1, 2});
        for (int i = 0; i < 10; i++)
        {
            row.merge(Row.of(articleAttribute, i, new Object[] {1, i}), -1);
        }

        //        Function<Row, Object> e = new BinaryExpression(
        //                new BinaryExpression(
        //                        new ColumnPathExpression("art_id"),
        //                        new ColumnPathExpression("idx_id"),
        //                        BinaryExpression.Operand.EQ),
        //                r -> true,
        //                BinaryExpression.Operand.OR);

        IEvaluator e = new DotExpression(
                new IdentifierExpression("aa"),
                new FilterExpression(new LambdaExpression(
                        new BinaryExpression(
                                new DotExpression(
                                        new IdentifierExpression("aa"),
                                        new IdentifierExpression("aa.sku_id")),
                                (ec, p) -> 5,
                                Operand.EQ),
                        asList("aa"))));

        System.out.println(IteratorUtils.toList(IteratorUtils.getIterator(e.evaluate(null, row))));
    }

    /** Definition of an evaluator */
    interface IEvaluator
    {
        /** Evaluate this node */
        Object evaluate(EvaluatorContext context, Object parent);
    }

    /** Context used during evaluation */
    static class EvaluatorContext
    {
        /** List of lambda */
        List<LambdaIdentifier> lambdaIdentifiers = new ArrayList<>();

        /** Returns or creates a new context */
        static EvaluatorContext get(EvaluatorContext context)
        {
            return context != null ? context : new EvaluatorContext();
        }

        void addLambdaIdentifier(LambdaIdentifier lambdaIdentifier)
        {
            lambdaIdentifiers.add(lambdaIdentifier);
        }
    }

    /** Expression node for a identifier */
    static class IdentifierExpression implements IEvaluator
    {
        private final String identifier;
        private ColumnPathProjection columnPathProjection;
        private int lambdaIndex = -2;

        IdentifierExpression(String identifier)
        {
            this.identifier = requireNonNull(identifier);
        }

        @Override
        public Object evaluate(EvaluatorContext context, Object obj)
        {
            verifyLambda(context);

            // Lambda identifier
            if (lambdaIndex != -1)
            {
                return context.lambdaIdentifiers.get(lambdaIndex).getValue();
            }
            if (!(obj instanceof Row))
            {
                throw new RuntimeException("ColumnPathExpression expected Row as input, got: " + obj);
            }

            return columnPathProjection.getValue((Row) obj);
        }

        private void verifyLambda(EvaluatorContext context)
        {
            if (lambdaIndex != -2)
            {
                return;
            }

            // No context -> no lambda
            if (context == null)
            {
                columnPathProjection = new ColumnPathProjection(identifier);
                lambdaIndex = -1;
                return;
            }

            int size = context.lambdaIdentifiers.size();
            for (int i = 0; i < size; i++)
            {
                if (identifier.equals(context.lambdaIdentifiers.get(i).identifier))
                {
                    lambdaIndex = i;
                    return;
                }
            }

            // No lambda found -> ColumnPathProjection
            columnPathProjection = new ColumnPathProjection(identifier);
            lambdaIndex = -1;
        }
    }

    /** Domain of a labmda expression */
    static class LambdaExpression implements IEvaluator
    {
        final IEvaluator expression;
        final List<String> identifiers;

        LambdaExpression(IEvaluator expression, List<String> identifiers)
        {
            this.expression = requireNonNull(expression);
            this.identifiers = requireNonNull(identifiers);
        }

        @Override
        public Object evaluate(EvaluatorContext context, Object obj)
        {
            throw new RuntimeException("Cannot evaluate a lambda expression");
        }
    }

    /** Expression of dot type */
    static class DotExpression implements IEvaluator
    {
        private final IEvaluator left;
        private final IEvaluator right;

        DotExpression(IEvaluator left, IEvaluator right)
        {
            this.left = requireNonNull(left);
            this.right = requireNonNull(right);
        }

        @Override
        public Object evaluate(EvaluatorContext context, Object obj)
        {
            Object leftValue = left.evaluate(context, obj);
            return right.evaluate(context, leftValue);
        }
    }

    /**
     * Expression that evaluates a lambda identifier
     *
     * <pre>
     *  map(a -> a.pos)
     *  Will evaluate a on the right hand side
     * </pre>
     **/
    static class LambdaIdentifier
    {
        private final String identifier;
        private final MutableObject<Object> value = new MutableObject<>();

        LambdaIdentifier(String identifier)
        {
            this.identifier = requireNonNull(identifier);
        }

        Object getValue()
        {
            return value.getValue();
        }

        void setValue(Object value)
        {
            this.value.setValue(value);
        }

        @Override
        public int hashCode()
        {
            return identifier.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof LambdaIdentifier)
            {
                return identifier.equals(((LambdaIdentifier) obj).identifier);
            }
            return false;
        }
    }

    /** Isnull function */
    static class IsNullExpression implements IEvaluator
    {
        private final List<IEvaluator> arguments;

        IsNullExpression(List<IEvaluator> arguments)
        {
            this.arguments = requireNonNull(arguments);
        }

        @Override
        public Object evaluate(EvaluatorContext context, Object parent)
        {
            for (IEvaluator arg : arguments)
            {
                Object value = arg.evaluate(context, parent);
                if (value != null)
                {
                    return value;
                }
            }
            return null;
        }
    }

    /** Expression for map function that is applied to a parent iterable */
    static class MapExpression implements IEvaluator
    {
        private final IEvaluator expression;
        private final String identifier;

        MapExpression(LambdaExpression lambdaExpression)
        {
            this.expression = requireNonNull(lambdaExpression).expression;
            if (lambdaExpression.identifiers.size() != 1)
            {
                throw new RuntimeException("map function takes one argument lambda expression");
            }
            this.identifier = lambdaExpression.identifiers.get(0);
        }

        @Override
        public Object evaluate(EvaluatorContext context, Object obj)
        {
            EvaluatorContext ctx = EvaluatorContext.get(context);
            LambdaIdentifier lambdaIdentifier = new LambdaIdentifier(identifier);
            ctx.addLambdaIdentifier(lambdaIdentifier);
            return new TransformIterator(IteratorUtils.getIterator(obj), input ->
            {
                lambdaIdentifier.setValue(input);
                // Evaluate with parent obj here to not loose context
                return expression.evaluate(ctx, obj);
            });
        }
    }

    /** Expression for filter function that is applied to a parent iterable */
    static class FilterExpression implements IEvaluator
    {
        private final IEvaluator expression;
        private final String identifier;

        FilterExpression(LambdaExpression lambdaExpression)
        {
            this.expression = requireNonNull(lambdaExpression).expression;
            if (lambdaExpression.identifiers.size() != 1)
            {
                throw new RuntimeException("filter function takes one argument lambda expression");
            }
            this.identifier = lambdaExpression.identifiers.get(0);
        }

        @Override
        public Object evaluate(EvaluatorContext context, Object obj)
        {
            EvaluatorContext ctx = EvaluatorContext.get(context);
            LambdaIdentifier lambdaIdentifier = new LambdaIdentifier(identifier);
            ctx.addLambdaIdentifier(lambdaIdentifier);
            return new FilterIterator(IteratorUtils.getIterator(obj), input ->
            {
                lambdaIdentifier.setValue(input);
                // Evaluate with parent obj here to not loose context
                Object value = expression.evaluate(ctx, obj);
                if (!(value instanceof Boolean))
                {
                    throw new RuntimeException("Expected boolean result from filter predicate, got: " + value);
                }
                return ((Boolean) value).booleanValue();
            });
        }
    }

    static class BinaryExpression implements IEvaluator
    {
        private final IEvaluator left;
        private final IEvaluator right;
        private final Operand operand;

        BinaryExpression(IEvaluator left, IEvaluator right, Operand operand)
        {
            this.left = requireNonNull(left);
            this.right = requireNonNull(right);
            this.operand = requireNonNull(operand);
        }

        @Override
        public Object evaluate(EvaluatorContext context, Object obj)
        {
            Object leftValue = left.evaluate(context, obj);
            Object rightValue = right.evaluate(context, obj);

            switch (operand)
            {
                case ADD:
                    break;
                case AND:
                    if (leftValue instanceof Boolean && rightValue instanceof Boolean)
                    {
                        return ((Boolean) leftValue).booleanValue() && ((Boolean) rightValue).booleanValue();
                    }
                    break;
                case OR:
                    if (leftValue instanceof Boolean && rightValue instanceof Boolean)
                    {
                        return ((Boolean) leftValue).booleanValue() || ((Boolean) rightValue).booleanValue();
                    }
                    break;
                case SUBSTRACT:
                    break;
                case EQ:
                    return Objects.equals(leftValue, rightValue);
                default:
                    break;

            }

            return null;
        }

        enum Operand
        {
            ADD,
            SUBSTRACT,
            OR,
            AND,
            EQ;
        }
    }

    @Test
    public void test()
    {
        Operator root = operator3();
        System.out.println("Logical Plan " + System.lineSeparator() + root.toString(1));
        for (int l = 0; l < 1000; l++)
        {
            long time = System.currentTimeMillis();
            int rowCount = 0;
            Iterator<Row> it = root.open(new OperatorContext());
            while (it.hasNext())
            {
                Row next = it.next();

                //                System.out.println(next.getPos());
                //                Object o = next.getObject(0);
                //                if (o instanceof Iterable)
                //                {
                //                    float sum = 0;
                //                    for (Object v : (Iterable) o)
                //                    {
                //                        System.out.println(v);
                ////                        sum += (float) v;
                //                    }
                //
                //                }
                //                System.out.println(next);
                //                checkDup(next);
                rowCount++;
            }

            System.out.println("Row count: " + rowCount + " Memory: " + FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) + " Time: "
                + (System.currentTimeMillis() - time));
        }
    }

    //    void checkDup(Row row)
    //    {
    //        if (row.childRows != null)
    //        {
    //            for (List<Row> li : row.childRows)
    //            {
    //                TIntHashSet s = new TIntHashSet();
    //                if (li.stream().anyMatch(p -> !s.add(p.pos)))
    //                {
    //                    throw new RuntimeException("Duplicate " + li.get(0).meta.table);
    //                }
    //
    //                li.forEach(r -> checkDup(r));
    //            }
    //        }
    //    }

    /** Projection operator that scans a parent rows alias */
    static class AliasScan implements Operator
    {
        private final int aliasIndex;

        AliasScan(int aliasIndex)
        {
            this.aliasIndex = aliasIndex;
            if (aliasIndex < 0)
            {
                throw new RuntimeException("Alias index must be positive");
            }
        }

        @Override
        public Iterator<Row> open(OperatorContext context)
        {
            Row row = context.getParentProjectionRow();
            if (row == null)
            {
                throw new RuntimeException("Parent row must be set in a projection selection operator");
            }

            List<Row> childRows = row.getChildRows(aliasIndex);
            if (childRows == null)
            {
                throw new RuntimeException("Could not find a child alias with index " + aliasIndex + " in table " + row.getTableAlias());
            }

            return childRows.iterator();
        }
    }

    /** Query consisting of a select and a projection */
    static class Query
    {
        final Operator selection;
        final ObjectProjection projection;

        Query(Operator operator, Map<String, Projection> projections)
        {
            this.selection = requireNonNull(operator);
            this.projection = new ObjectProjection(projections);
        }

        // TODO: Add object writer interface to avoid alot of allocations
        // if streaming is wanted
        public List<Map<String, Object>> executeToMap()
        {
            StopWatch sw = new StopWatch();
            sw.start();
            MapWriter writer = new MapWriter();
            OperatorContext context = new OperatorContext();
            List<Map<String, Object>> rowResults = new ArrayList<>();
            int rowCount = 0;
            Iterator<Row> it = selection.open(context);
            while (it.hasNext())
            {
                Row row = it.next();
                projection.writeValue(writer, row);
                ////                rowResults.add(writer.getAndReset());
                writer.sb.deleteCharAt(writer.sb.length() - 1);
                ////                System.out.println(writer.sb.toString());
                writer.sb = new StringBuilder();
                rowCount++;
            }

            sw.stop();

            System.out.println("Rows/s: " + (rowCount / ((float) sw.getTime() / 1000)));

            return rowResults;
        }
    }

    static class MapWriter implements OutputWriter
    {
        //        Stack<Map<String, Object>> objectStack = new Stack<>();
        //        Stack<List<Object>> listStack = new Stack<>();
        //
        //        String currentField;

        StringBuilder sb = new StringBuilder();

        Map<String, Object> getAndReset()
        {
            return null;
        }

        //        Map<String, Object> getCurrent()
        //        {
        //            // Root is always
        ////            if (objectStack.isEmpty())
        ////            {
        ////                objectStack.add(new HashMap<>());
        ////            }
        ////
        ////            return objectStack.peek();
        //        }

        @Override
        public void writeFieldName(String name)
        {
            sb.append("\"").append(name).append("\":");
            //            currentField = name;
        }

        @Override
        public void writeValue(Object value)
        {
            // Target?
            // Array or object

            //            getCurrent().put(currentField, value);

            if (value instanceof String)
            {
                sb.append("\"");
            }
            sb.append(value);
            if (value instanceof String)
            {
                sb.append("\"");
            }

            sb.append(",");
        }

        @Override
        public void startObject()
        {
            sb.append("{");

            //            insideObject = true;
            //            objectStack.push(new HashMap<>());
        }

        @Override
        public void endObject()
        {
            sb.deleteCharAt(sb.length() - 1);
            sb.append("}");
            sb.append(",");
            //            if (objectStack.size() > 1)
            //            {
            //                objectStack.pop();
            //            }

            //            insideObjectPrev = insideObject;
            //            insideObject = insideObjectPrev;
        }

        @Override
        public void startArray()
        {
            sb.append("[");
            //            listStack.push(new ArrayList<>());
        }

        @Override
        public void endArray()
        {
            sb.deleteCharAt(sb.length() - 1);
            sb.append("]");
            sb.append(",");
            //            listStack.pop();
        }

    }

    /** Output result for a row/tuple */
    //    static class Result
    //    {
    //        String[] columns;
    //        Object[] values;
    //
    //        @Override
    //        public String toString()
    //        {
    //            StringBuilder sb = new StringBuilder();
    //            sb.append(ArrayUtils.toString(columns));
    //
    //            if (values != null)
    //            {
    //                sb.append(" {");
    //                for (Object v : values)
    //                {
    //                    if (v == null)
    //                    {
    //                        sb.append("null");
    //                    }
    //                    else if (v instanceof Iterable)
    //                    {
    //                        sb.append("[");
    //                        for (Object o : (Iterable) v)
    //                        {
    //                            sb.append(ArrayUtils.toString(o));
    //                            sb.append(", ");
    //                        }
    //                        sb.append("]");
    //                        continue;
    //                    }
    //                    else
    //                    {
    //                        sb.append(v);
    //                    }
    //                    sb.append(" ");
    //                }
    //                sb.append("}");
    //            }
    //            ;
    //
    //            return sb.toString();
    //        }
    //    }

    public static class ListScan implements Operator
    {
        private final TableAlias meta;
        private final Supplier<List<Object[]>> valuesSupplier;

        public ListScan(TableAlias meta, Supplier<List<Object[]>> valuesSupplier)
        {
            this.meta = meta;
            this.valuesSupplier = valuesSupplier;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Iterator<Row> open(OperatorContext context)
        {
            final List<Object[]> tuples = valuesSupplier.get();
            return new TransformIterator(tuples.iterator(), new Transformer()
            {
                int position = 0;

                @Override
                public Object transform(Object input)
                {
                    return Row.of(meta, position++, (Object[]) input);
                }
            });
        }

        @Override
        public String toString(int indent)
        {
            return "SCAN (" + meta.getTable() + ")";
        }
    }
}
