package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.catalog.OperatorFactory;
import com.viskan.payloadbuilder.catalog.TableFunctionInfo;
import com.viskan.payloadbuilder.operator.ArrayProjection;
import com.viskan.payloadbuilder.operator.CachingOperator;
import com.viskan.payloadbuilder.operator.DefaultRowMerger;
import com.viskan.payloadbuilder.operator.ExpressionHashFunction;
import com.viskan.payloadbuilder.operator.ExpressionOperator;
import com.viskan.payloadbuilder.operator.ExpressionPredicate;
import com.viskan.payloadbuilder.operator.ExpressionProjection;
import com.viskan.payloadbuilder.operator.Filter;
import com.viskan.payloadbuilder.operator.HashMatch;
import com.viskan.payloadbuilder.operator.JsonStringWriter;
import com.viskan.payloadbuilder.operator.NestedLoop;
import com.viskan.payloadbuilder.operator.ObjectProjection;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.operator.Projection;
import com.viskan.payloadbuilder.operator.RowSpool;
import com.viskan.payloadbuilder.operator.RowSpoolScan;
import com.viskan.payloadbuilder.operator.TableFunctionOperator;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;
import com.viskan.payloadbuilder.parser.tree.Query;

import static com.viskan.payloadbuilder.parser.tree.QualifiedName.of;
import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/** Test of {@link OperatorVisitor} */
public class OperatorBuilderTest extends Assert
{
    private final QueryParser parser = new QueryParser();
    private final CatalogRegistry catalogRegistry = new CatalogRegistry();

    @Test
    public void test_invalid_alias_hierarchy()
    {
        catalogRegistry.getDefault().setOperatorFactory(new OperatorFactory()
        {
            @Override
            public boolean requiresParents(QualifiedName qname)
            {
                return false;
            }

            @Override
            public Operator create(QualifiedName qname, TableAlias tableAlias)
            {
                return c -> emptyIterator();
            }
        });
        Query query = parser.parseQuery(catalogRegistry, "select a from tableA a inner join tableB a on a.id = a.id ");
        try
        {
            OperatorBuilder.create(catalogRegistry, query);
            fail("Alias already exists in parent hierarchy");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("defined multiple times for parent"));
        }

        query = parser.parseQuery(catalogRegistry, "select a from tableA a inner join [tableB] b on b.id = a.id inner join [tableC] b on b.id = a.id");
        try
        {
            OperatorBuilder.create(catalogRegistry, query);
            fail("defined multiple times for parent");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("defined multiple times for parent"));
        }
    }

    @Test
    public void test_table_function()
    {
        String query = "select r.Value * r1.Value * r2.Value mul, r.Value r, r1.filter(x -> x.Value > 10).map(x -> x.Value) r1, r2.Value r2, array(Value from r1) r1A from range(randomInt(100), randomInt(100) + 100) r inner join [range(randomInt(100))] r1 on r1.Value <= r.Value inner join range(randomInt(100), randomInt(100) + 100) r2 on r2.Value = r.Value";
        QueryResult queryResult = getQueryResult(query);

        TableFunctionInfo range = (TableFunctionInfo) catalogRegistry.getDefault().getFunction("range");
        QualifiedName rangeQname = QualifiedName.of("DEFAULT", "range");

        TableAlias r = TableAlias.of(null, rangeQname, "r");
        r.setColumns(new String[] {"Value"});
        TableAlias r1 = TableAlias.of(r, rangeQname, "r1");
        r1.setColumns(new String[] {"Value"});
        TableAlias r2 = TableAlias.of(r, rangeQname, "r2");
        r2.setColumns(new String[] {"Value"});

        Operator expected = new HashMatch(
                "",
                new NestedLoop(
                        "",
                        new TableFunctionOperator(r, range, asList(
                                e("randomInt(100)"),
                                e("randomInt(100) + 100"))),
                        new CachingOperator(new TableFunctionOperator(r1, range, asList(
                                e("randomInt(100)")))),
                        new ExpressionPredicate(e("r1.Value <= r.Value")),
                        DefaultRowMerger.DEFAULT,
                        true,
                        false),
                new TableFunctionOperator(r2, range, asList(
                        e("randomInt(100)"),
                        e("randomInt(100) + 100"))),
                new ExpressionHashFunction(asList(e("r.Value"))),
                new ExpressionHashFunction(asList(e("r2.Value"))),
                new ExpressionPredicate(e("r2.Value = r.Value")),
                DefaultRowMerger.DEFAULT,
                false,
                false);

        //                System.out.println(expected.toString(1));
        //                System.out.println(queryResult.operator.toString(1));

        assertEquals(expected, queryResult.operator);

        Projection expectedProjection = new ObjectProjection(ofEntries(true,
                entry("mul", new ExpressionProjection(parser.parseExpression(catalogRegistry, "r.Value * r1.Value * r2.Value"))),
                entry("r", new ExpressionProjection(parser.parseExpression(catalogRegistry, "r.Value"))),
                entry("r1", new ExpressionProjection(parser.parseExpression(catalogRegistry, "r1.filter(x -> x.Value > 10).map(x -> x.Value)"))),
                entry("r2", new ExpressionProjection(parser.parseExpression(catalogRegistry, "r2.Value"))),
                entry("r1A", new ArrayProjection(asList(
                        new ExpressionProjection(parser.parseExpression(catalogRegistry, "Value"))), new ExpressionOperator(parser.parseExpression(catalogRegistry, "r1"))))));

        assertEquals(expectedProjection, queryResult.projection);
    }

    @Test
    public void test_mixed_populate()
    {
        String query = "select aa.sku_id "
            + "from source s "
            + "inner join "
            + "["
            + "  article "
            + "] a "
            + "  on a.art_id = s.art_id "
            + "inner join "
            + "["
            + "  articleAttribute aa"
            + "  inner join articlePrice ap"
            + "    on ap.sku_id = aa.sku_id"
            + "  inner join "
            + "  ["
            + "    attribute1 "
            + "  ] a1 "
            + "    on a1.attr1_id = aa.attr1_id "
            + "  where active_flg "
            + "  and ap.price_sales > 0 "
            + "] aa "
            + "  on aa.art_id = s.art_id"
            + "   "
            + "";

        QueryResult queryResult = getQueryResult(query);

        TableAlias source = new TableAlias(null, of("source"), "s", new String[] {"art_id"});
        new TableAlias(source, of("article"), "a", new String[] {"art_id"});
        TableAlias articleAttribute = new TableAlias(source, of("articleAttribute"), "aa", new String[] {"sku_id", "attr1_id", "art_id", "active_flg"});
        new TableAlias(articleAttribute, of("articlePrice"), "ap", new String[] {"price_sales", "sku_id"});
        new TableAlias(articleAttribute, of("attribute1"), "a1", new String[] {"attr1_id"});

        //                                System.out.println(source.printHierarchy(0));
        //                                System.out.println(queryResult.aliases.get(0).printHierarchy(0));

        assertTrue("Alias hierarchy should be equal", source.isEqual(queryResult.aliases.get(0)));

        Operator expected = new RowSpool("parents",
                new RowSpool("parents",
                        queryResult.tableOperators.get(0),
                        new HashMatch(
                                "",
                                new RowSpoolScan("parents"),
                                queryResult.tableOperators.get(1),
                                new ExpressionHashFunction(asList(e("s.art_id"))),
                                new ExpressionHashFunction(asList(e("a.art_id"))),
                                new ExpressionPredicate(e("a.art_id = s.art_id")),
                                DefaultRowMerger.DEFAULT,
                                true,
                                false)),
                new HashMatch(
                        "",
                        new RowSpoolScan("parents"),
                        new Filter(
                                new RowSpool(
                                        "parents",
                                        new RowSpool(
                                                "parents",
                                                new Filter(queryResult.tableOperators.get(2), new ExpressionPredicate(e("active_flg"))),
                                                new HashMatch(
                                                        "",
                                                        new RowSpoolScan("parents"),
                                                        queryResult.tableOperators.get(3),
                                                        new ExpressionHashFunction(asList(e("aa.sku_id"))),
                                                        new ExpressionHashFunction(asList(e("ap.sku_id"))),
                                                        new ExpressionPredicate(e("ap.sku_id = aa.sku_id")),
                                                        DefaultRowMerger.DEFAULT,
                                                        false,
                                                        false)),
                                        new HashMatch(
                                                "",
                                                new RowSpoolScan("parents"),
                                                queryResult.tableOperators.get(4),
                                                new ExpressionHashFunction(asList(e("aa.attr1_id"))),
                                                new ExpressionHashFunction(asList(e("a1.attr1_id"))),
                                                new ExpressionPredicate(e("a1.attr1_id = aa.attr1_id")),
                                                DefaultRowMerger.DEFAULT,
                                                true,
                                                false)),
                                new ExpressionPredicate(e("ap.price_sales > 0"))),
                        new ExpressionHashFunction(asList(e("s.art_id"))),
                        new ExpressionHashFunction(asList(e("aa.art_id"))),
                        new ExpressionPredicate(e("aa.art_id = s.art_id")),
                        DefaultRowMerger.DEFAULT,
                        true,
                        false));

        //        System.err.println(expected.toString(1));
        //        System.out.println(queryResult.operator.toString(1));

        assertEquals(expected, queryResult.operator);

        Projection expectedProjection = new ObjectProjection(
                ofEntries(true, entry("sku_id", new ExpressionProjection(e("aa.sku_id")))));

        assertEquals(expectedProjection, queryResult.projection);
    }

    @Ignore
    @Test
    public void test()
    {
        Query query = parser.parseQuery(catalogRegistry,
                "select aa.a1.attr1_code"
                    + ",    s.art_id + ab.articleBrandId col"
                    + "/*,    array"
                    + "     ("
                    + "       object"
                    + "       ("
                    + "         rowId, "
                    + "         brandName, "
                    + "         articleBrandId "
                    + "       )"
                    + "       from ab "
                    + "     ) brands*/"
                    + ",    array"
                    + "     ("
                    + "         sku_id,"
                    + "         array"
                    + "         ("
                    + "             ap.sku_id,"
                    + "             ap.row_id "
                    + "             from ap"
                    + "         ) "
                    + "     from aa"
                    + "     ) sub "
                    + "from source s "
                    + "inner join "
                    + "["
                    + "  article "
                    + "] a "
                    + "    on a.art_id = s.art_id "
                    + "inner join "
                    + "["
                    + "  articleAttribute aa"
                    + "  inner join "
                    + "  ["
                    + "    attribute1 a1 "
                    + "    inner join attribute1Group a1g "
                    + "     on a1g.attr1_id = a1.attr1_id "
                    + "  ] a1 "
                    + "      on a1.attr1_id = aa.attr1_id"
                    + "  inner join articlePrice ap "
                    + "    on ap.sku_id = aa.sku_id "
                    + "    and ap.active_flg "
                    + "    and ap.min_qty = 1"
                    + "] aa "
                    + "    on aa.art_id = s.art_id "
                    + "    and aa.active_flg "
                    + "    and aa.internet_flg "
                    + "inner join "
                    + "["
                    + "  articleBrand "
                    + "] ab "
                    + "    on ab.articleBrandId = a.articleBrandId "
                    + "");

        catalogRegistry.getDefault().setOperatorFactory(new OperatorFactory()
        {
            @Override
            public boolean requiresParents(QualifiedName qname)
            {
                return true;//qname.toString().equals("articleAttribute");
            }

            @Override
            public Operator create(QualifiedName qname, TableAlias tableAlias)
            {
                return new CachingOperator(new Operator()
                {
                    @Override
                    public Iterator<Row> open(OperatorContext c)
                    {
                        Random rand = new Random();
                        //                        System.out.println(tableAlias + " " + Arrays.toString(tableAlias.getColumns()));
                        //                System.out.println("Parents: " + c.getSpoolRows("parents"));
                        //                System.out.println();

                        boolean ap = tableAlias.getAlias().equals("ap");
                        boolean ab = tableAlias.getAlias().equals("ab");

                        int to = 50000;
                        return IntStream.range(0, rand.nextInt(to) + 1)
                                .mapToObj(i ->
                                {
                                    int length = tableAlias.getColumns().length;
                                    Object[] values = new Object[length];
                                    for (int ii = 0; ii < length; ii++)
                                    {
                                        if (tableAlias.getColumns()[ii].endsWith("flg"))
                                        {
                                            values[ii] = Boolean.TRUE;
                                        }
                                        else if (ap && tableAlias.getColumns()[ii].equals("sku_id"))
                                        {
                                            values[ii] = i % 10;
                                        }
                                        else if (ab && tableAlias.getColumns()[ii].equals("articleBrandId"))
                                        {
                                            values[ii] = i % 10;
                                        }
                                        else if (tableAlias.getColumns()[ii].equals("min_qty"))
                                        {
                                            values[ii] = 1;
                                        }
                                        else if (tableAlias.getColumns()[ii].endsWith("Name"))
                                        {
                                            byte[] b = new byte[10];
                                            rand.nextBytes(b);
                                            values[ii] = new String(b);
                                        }
                                        else
                                        {
                                            values[ii] = i;
                                        }
                                    }

                                    return Row.of(tableAlias, i, values);
                                })
                                .iterator();
                    }

                    @Override
                    public String toString()
                    {
                        return tableAlias.getTable().toString();
                    }
                });
            }
        });

        for (int i = 0; i < 1000; i++)
        {

            StopWatch sw = new StopWatch();
            sw.start();

            Pair<Operator, Projection> pair = OperatorBuilder.create(catalogRegistry, query);
            Operator operator = pair.getLeft();
            Projection projection = pair.getRight();
            if (i == 0)
            {
                System.err.println(operator.toString(1));
            }
            JsonStringWriter writer = new JsonStringWriter();

            OperatorContext context = new OperatorContext();
            Iterator<Row> it = operator.open(context);
            int count = 0;
            while (it.hasNext())
            {
                Row row = it.next();
                projection.writeValue(writer, context, row);
                writer.getAndReset();
                //            System.out.println(row);
                //            System.out.println();
                count++;
            }

            sw.stop();
            System.out.println("TIme: " + sw.toString() + ", rows: " + count + ", mem: " + FileUtils.byteCountToDisplaySize((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())));

        }
    }

    @Test
    public void test_columns_collecting()
    {
        String query = "select r.a1.attr1_code"
            + ", art_id"
            + ", idx_id"
            + ", object"
            + "  ("
            + "    pluno, "
            + "    object "
            + "    ("
            + "      a1.rgb_code"
            + "    ) attribute1, "
            + "    object "
            + "    ("
            + "      a1.colorGroup "
            + "      from a1 "
            + "      where a1.group_flg "
            + "    ) attribute1Group "
            + "    from aa "
            + "    order by aa.internet_date_start"
            + "  ) obj "
            + ", array"
            + "  ("
            + "    attr2_code "
            + "    from aa.map(aa -> aa.a2) "
            + "    where aa.ean13 != ''"
            + "  ) arr "
            + ", array"
            + "  ("
            + "    art_id,"
            + "    note_id "
            + "    from aa.concat(aa.ap)"
            + "  ) arr2 "
            + "from article a "
            + "inner join "
            + "["
            + "  articleAttribute aa "
            + "  inner join "
            + "  ["
            + "    articlePrice "
            + "  ] ap "
            + "    on ap.sku_id = aa.sku_id "
            + "    and ap.price_sales > 0 "
            + "  inner join "
            + "  ["
            + "    attribute1 "
            + "  ] a1 "
            + "    on a1.attr1_id = aa.attr1_id "
            + "    and a1.lang_id = 1 "
            + "  inner join "
            + "  ["
            + "    attribute2 "
            + "  ] a2 "
            + "    on a2.attr2_id = aa.attr2_id "
            + "    and a2.lang_id = 1 "
            + "  inner join "
            + "  ["
            + "    attribute3 "
            + "  ] a3 "
            + "    on a3.attr3_id = aa.attr3_id "
            + "    and a3.lang_id = 1 "
            + "  where ap.price_org > 0"
            + "  order by a2.attr2_no "
            + "] aa "
            + "  on aa.art_id = a.art_id "
            + "  and aa.active_flg "
            + "  and aa.internet_flg "
            + "inner join "
            + "["
            + "  articleProperty "
            + "  group by propertykey_id "
            + "] ap "
            + "  on ap.art_id = a.art_id "
            + "cross apply "
            + "["
            + "  range(10) r "
            + "  inner join "
            + "  ["
            + "    attribute1 "
            + "  ] a1 "
            + "      on a1.someId = r.Value "
            + "] r "
            + "where not a.add_on_flg and a.articleType = 'regular' "
            + "group by a.note_id "
            + "order by a.stamp_dat_cr";

        QueryResult result = getQueryResult(query);

        TableAlias article = new TableAlias(null, of("article"), "a", new String[] {"stamp_dat_cr", "art_id", "add_on_flg", "articleType", "note_id", "idx_id"});
        TableAlias articleAttribute = new TableAlias(article, of("articleAttribute"), "aa",
                new String[] {"internet_flg", "internet_date_start", "sku_id", "attr1_id", "art_id", "pluno", "active_flg", "ean13", "attr3_id", "note_id", "attr2_id"});
        new TableAlias(articleAttribute, of("articlePrice"), "ap", new String[] {"price_sales", "sku_id", "art_id", "price_org", "note_id"});
        new TableAlias(articleAttribute, of("attribute1"), "a1", new String[] {"colorGroup", "attr1_id", "rgb_code", "lang_id", "group_flg"});
        new TableAlias(articleAttribute, of("attribute2"), "a2", new String[] {"attr2_code", "lang_id", "attr2_no", "attr2_id"});
        new TableAlias(articleAttribute, of("attribute3"), "a3", new String[] {"lang_id", "attr3_id"});
        new TableAlias(article, of("articleProperty"), "ap", new String[] {"propertykey_id", "art_id"});
        TableAlias range = new TableAlias(article, of("DEFAULT", "range"), "r", new String[] {"Value"});
        new TableAlias(range, of("attribute1"), "a1", new String[] {"someId", "attr1_code"});

        //                                System.out.println(article.printHierarchy(1));
        //                                System.out.println(result.aliases.get(0).printHierarchy(1));

        assertTrue("Alias hierarchy should be equal", article.isEqual(result.aliases.get(0)));
    }

    @Test
    public void test_single_table()
    {
        String query = "select s.id1, a.id2 from source s";
        QueryResult result = getQueryResult(query);

        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"a", "id1"});
        assertTrue(source.isEqual(result.aliases.get(0)));

        assertEquals(result.tableOperators.get(0), result.operator);
        assertEquals(new ObjectProjection(ofEntries(true,
                entry("id1", new ExpressionProjection(parser.parseExpression(catalogRegistry, "s.id1"))),
                entry("id2", new ExpressionProjection(parser.parseExpression(catalogRegistry, "a.id2"))))),
                result.projection);
    }

    @Test
    public void test_select_item_with_filter()
    {
        String query = "select object(s.id1, a.id2 from s where s.id4 > 0) arr from source s inner join [article where note_id > 0] a on a.art_id = s.art_id and a.active_flg where s.id3 > 0";
        QueryResult result = getQueryResult(query);

        // Assert aliaes
        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"art_id", "id4", "id3", "id1"});
        new TableAlias(source, QualifiedName.of("article"), "a", new String[] {"art_id", "active_flg", "id2", "note_id"});

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.aliases.get(0)));

        Operator expected = new RowSpool("parents",
                new Filter(result.tableOperators.get(0), new ExpressionPredicate(e("s.id3 > 0"))),
                new HashMatch(
                        "",
                        new RowSpoolScan("parents"),
                        new Filter(result.tableOperators.get(1), new ExpressionPredicate(e("a.active_flg and note_id > 0"))),
                        new ExpressionHashFunction(asList(e("s.art_id"))),
                        new ExpressionHashFunction(asList(e("a.art_id"))),
                        new ExpressionPredicate(e("a.art_id = s.art_id")),
                        DefaultRowMerger.DEFAULT,
                        true,
                        false));

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(ofEntries(true,
                        entry("arr", new ObjectProjection(ofEntries(true,
                                entry("id1", new ExpressionProjection(e("s.id1"))),
                                entry("id2", new ExpressionProjection(e("a.id2")))),
                                new Filter(
                                        new ExpressionOperator(e("s")),
                                        new ExpressionPredicate(e("s.id4 > 0"))))))),
                result.projection);
    }

    @Test
    public void test_correlated()
    {
        String query = "SELECT s.art_id "
            + "FROM source s "
            + "INNER JOIN "
            + "["
            + "  article a"
            + "  INNER JOIN [articleAttribute] aa"
            + "    ON aa.art_id = a.art_id "
            + "    AND s.id "
            + "] a"
            + "  ON a.art_id = s.art_id";

        QueryResult result = getQueryResult(query);

        // Assert aliaes
        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"id", "art_id"});
        TableAlias article = new TableAlias(source, QualifiedName.of("article"), "a", new String[] {"art_id"});
        new TableAlias(article, QualifiedName.of("articleAttribute"), "aa", new String[] {"art_id"});

        //        System.out.println(source.printHierarchy(1));
        //        System.out.println(result.aliases.get(0).printHierarchy(1));
        //
        assertTrue(source.isEqual(result.aliases.get(0)));

        Operator expected = new RowSpool("parents",
                result.tableOperators.get(0),
                // Correlated => nested loop
                new NestedLoop(
                        "",
                        new RowSpoolScan("parents"),
                        new RowSpool("parents",
                                result.tableOperators.get(1),
                                new HashMatch(
                                        "",
                                        new RowSpoolScan("parents"),
                                        result.tableOperators.get(2),
                                        new ExpressionHashFunction(asList(e("a.art_id"))),
                                        new ExpressionHashFunction(asList(e("aa.art_id"))),
                                        new ExpressionPredicate(e("aa.art_id = a.art_id AND s.id")),
                                        DefaultRowMerger.DEFAULT,
                                        true,
                                        false)),
                        new ExpressionPredicate(e("a.art_id = s.art_id")),
                        DefaultRowMerger.DEFAULT,
                        true,
                        false));

        //        System.err.println(expected.toString(1));
        //        System.out.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(ofEntries(true,
                        entry("art_id", new ExpressionProjection(e("s.art_id"))))),
                result.projection);
    }

    @Ignore
    @Test
    public void test_correlated_manual()
    {
        String query = "SELECT s.art_id "
            + "FROM source s "
            + "INNER JOIN "
            + "["
            + "  article a"
            + "  INNER JOIN [articleAttribute] aa"
            + "    ON aa.art_id = a.art_id "
            + "    AND s.id "
            + "] a"
            + "  ON a.art_id = s.art_id";

        catalogRegistry.getDefault().setOperatorFactory(new OperatorFactory()
        {
            Random rnd = new Random();

            @Override
            public boolean requiresParents(QualifiedName qname)
            {
                return false;
            }

            @Override
            public Operator create(QualifiedName qname, TableAlias tableAlias)
            {
                if (qname.toString().equals("source"))
                {
                    return new CachingOperator(c -> IntStream.range(0, 100).mapToObj(i -> Row.of(tableAlias, i, new Object[] { rnd.nextBoolean(), rnd.nextInt(100)})).iterator());
                }
                else if (qname.toString().equals("article"))
                {
                    return new CachingOperator(c -> IntStream.range(0, 1000).mapToObj(i -> Row.of(tableAlias, i, new Object[] {rnd.nextInt(1000)})).iterator());
                }

                return new CachingOperator(c -> IntStream.range(0, 10000).mapToObj(i -> Row.of(tableAlias, i, new Object[] {rnd.nextInt(1000)})).iterator());
            }
        });

        Pair<Operator, Projection> pair = null;
        for (int i = 0; i < 10; i++)
        {
            pair = OperatorBuilder.create(catalogRegistry, parser.parseQuery(catalogRegistry, query));
            OperatorContext context = new OperatorContext();
            Iterator<Row> it = pair.getKey().open(context);
            StopWatch sw = new StopWatch();
            sw.start();
            int count = 0;
            while (it.hasNext())
            {
                Row row = it.next();
                //            System.out.println(row);
                count++;
            }
            sw.stop();
            System.out.println("Time: " + sw.toString() + ", rows: " + count);
            System.out.println(FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
        }

        System.out.println(pair.getKey().toString(1));

    }

    protected Expression e(String expression)
    {
        return parser.parseExpression(catalogRegistry, expression);
    }

    protected QueryResult getQueryResult(String query)
    {
        List<Operator> tableOperators = new ArrayList<>();
        List<TableAlias> aliases = new ArrayList<>();
        catalogRegistry.getDefault().setOperatorFactory(new OperatorFactory()
        {
            @Override
            public boolean requiresParents(QualifiedName qname)
            {
                return true;
            }

            @Override
            public Operator create(QualifiedName qname, TableAlias tableAlias)
            {
                if (!aliases.contains(tableAlias))
                {
                    aliases.add(tableAlias);
                }
                Operator op = new Operator()
                {
                    @Override
                    public Iterator<Row> open(OperatorContext context)
                    {
                        return emptyIterator();
                    }

                    @Override
                    public String toString()
                    {
                        return qname.toString();
                    }
                };

                tableOperators.add(op);

                return op;
            }
        });

        Pair<Operator, Projection> pair = OperatorBuilder.create(catalogRegistry, parser.parseQuery(catalogRegistry, query));
        QueryResult result = new QueryResult();
        result.aliases = aliases;
        result.operator = pair.getLeft();
        result.projection = pair.getRight();
        result.tableOperators = tableOperators;
        return result;
    }

    static class QueryResult
    {
        List<Operator> tableOperators;

        Operator operator;
        Projection projection;
        List<TableAlias> aliases;
    }
}
