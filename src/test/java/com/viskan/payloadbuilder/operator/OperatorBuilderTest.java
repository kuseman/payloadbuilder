package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.catalog.TableFunctionInfo;
import com.viskan.payloadbuilder.operator.PredicateAnalyzer.AnalyzePair;
import com.viskan.payloadbuilder.operator.PredicateAnalyzer.AnalyzeResult;
import com.viskan.payloadbuilder.parser.Expression;
import com.viskan.payloadbuilder.parser.QualifiedName;
import com.viskan.payloadbuilder.parser.SortItem;
import com.viskan.payloadbuilder.parser.SortItem.NullOrder;
import com.viskan.payloadbuilder.parser.SortItem.Order;

import static com.viskan.payloadbuilder.parser.QualifiedName.of;
import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Ignore;
import org.junit.Test;

/** Test of {@link OperatorBuilder} */
public class OperatorBuilderTest extends AOperatorTest
{
    @Test
    public void test_invalid_alias_hierarchy()
    {
        try
        {
            getQueryResult("select a from tableA a inner join tableB a on a.id = a.id ");
            fail("Alias already exists in parent hierarchy");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("defined multiple times for parent"));
        }

        try
        {
            getQueryResult("select a from tableA a inner join [tableB] b on b.id = a.id inner join [tableC] b on b.id = a.id");
            fail("defined multiple times for parent");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("defined multiple times for parent"));
        }
    }

    @Test
    public void test_batch_limit_operator()
    {
        String query = "select a.art_id from source s with (batch_limit=250) inner join article a on a.art_id = s.art_id";
        QueryResult queryResult = getQueryResult(query);

        Operator expected = new BatchRepeatOperator(
                4,
                1,
                new HashJoin(
                        3,
                        "",
                        new BatchLimitOperator(
                                1,
                                queryResult.tableOperators.get(0),
                                e("250")),
                        queryResult.tableOperators.get(1),
                        new ExpressionHashFunction(asList(e("s.art_id"))),
                        new ExpressionHashFunction(asList(e("a.art_id"))),
                        new ExpressionPredicate(e("a.art_id = s.art_id")),
                        DefaultRowMerger.DEFAULT,
                        false,
                        false));

        //                System.out.println(queryResult.operator.toString(1));
        //                System.err.println(expected.toString(1));

        assertEquals(expected, queryResult.operator);
    }

    @Test
    public void test_sortBy()
    {
        String query = "select a.art_id from article a order by a.art_id";
        QueryResult queryResult = getQueryResult(query);

        Operator expected = new SortByOperator(
                1,
                queryResult.tableOperators.get(0),
                new ExpressionRowComparator((asList(new SortItem(e("a.art_id"), Order.ASC, NullOrder.UNDEFINED)))));

        //                System.out.println(queryResult.operator.toString(1));
        //                System.err.println(expected.toString(1));

        assertEquals(expected, queryResult.operator);
    }

    @Test
    public void test_groupBy()
    {
        String query = "select a.art_id from article a group by a.art_id";
        QueryResult queryResult = getQueryResult(query);

        Operator expected = new GroupByOperator(
                1,
                queryResult.tableOperators.get(0),
                asList("art_id"),
                new ExpressionValuesExtractor(asList(e("a.art_id"))),
                1);

        //        System.out.println(queryResult.operator.toString(1));
        //        System.err.println(expected.toString(1));

        assertEquals(expected, queryResult.operator);
    }

    @Test
    public void test_table_function()
    {
        String query = "select r.Value * r1.Value * r2.Value mul, r.Value r, r1.filter(x -> x.Value > 10).map(x -> x.Value) r1, r2.Value r2, array(Value from r1) r1A from range(randomInt(100), randomInt(100) + 100) r inner join [range(randomInt(100))] r1 on r1.Value <= r.Value inner join range(randomInt(100), randomInt(100) + 100) r2 on r2.Value = r.Value";
        QueryResult queryResult = getQueryResult(query);

        TableFunctionInfo range = (TableFunctionInfo) session.getCatalogRegistry().getBuiltin().getFunction("range");
        QualifiedName rangeQname = QualifiedName.of(range.getCatalog().getName(), "range");

        TableAlias r = TableAlias.of(null, rangeQname, "r");
        r.setColumns(new String[] {"Value"});
        TableAlias r1 = TableAlias.of(r, rangeQname, "r1");
        r1.setColumns(new String[] {"Value"});
        TableAlias r2 = TableAlias.of(r, rangeQname, "r2");
        r2.setColumns(new String[] {"Value"});

        Operator expected = new HashJoin(
                5,
                "",
                new NestedLoopJoin(
                        3,
                        "",
                        new TableFunctionOperator(0, r, range, asList(
                                e("randomInt(100)"),
                                e("randomInt(100) + 100"))),
                        new CachingOperator(2, new TableFunctionOperator(1, r1, range, asList(
                                e("randomInt(100)")))),
                        new ExpressionPredicate(e("r1.Value <= r.Value")),
                        DefaultRowMerger.DEFAULT,
                        true,
                        false),
                new TableFunctionOperator(4, r2, range, asList(
                        e("randomInt(100)"),
                        e("randomInt(100) + 100"))),
                new ExpressionHashFunction(asList(e("r.Value"))),
                new ExpressionHashFunction(asList(e("r2.Value"))),
                new ExpressionPredicate(e("r2.Value = r.Value")),
                DefaultRowMerger.DEFAULT,
                false,
                false);

        //                                        System.err.println(expected.toString(1));
        //                                        System.out.println(queryResult.operator.toString(1));

        assertEquals(expected, queryResult.operator);

        Projection expectedProjection = new ObjectProjection(asList("mul", "r", "r1", "r2", "r1A"), asList(
                new ExpressionProjection(e("r.Value * r1.Value * r2.Value")),
                new ExpressionProjection(e("r.Value")),
                new ExpressionProjection(e("r1.filter(x -> x.Value > 10).map(x -> x.Value)")),
                new ExpressionProjection(e("r2.Value")),
                new ArrayProjection(asList(
                        new ExpressionProjection(e("Value"))), new ExpressionOperator(6, e("r1")))));

        //                                System.err.println(expected.toString(1));
        //                                System.out.println(queryResult.operator.toString(1));

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

        assertTrue("Alias hierarchy should be equal", source.isEqual(queryResult.alias));

        Operator expected = new HashJoin(
                10,
                "",
                new HashJoin(
                        2,
                        "",
                        queryResult.tableOperators.get(0),
                        queryResult.tableOperators.get(1),
                        new ExpressionHashFunction(asList(e("s.art_id"))),
                        new ExpressionHashFunction(asList(e("a.art_id"))),
                        new ExpressionPredicate(e("a.art_id = s.art_id")),
                        DefaultRowMerger.DEFAULT,
                        true,
                        false),
                new FilterOperator(
                        9,
                        new HashJoin(
                                8,
                                "",
                                new HashJoin(
                                        6,
                                        "",
                                        new FilterOperator(4, queryResult.tableOperators.get(2), new ExpressionPredicate(e("active_flg"))),
                                        queryResult.tableOperators.get(3),
                                        new ExpressionHashFunction(asList(e("aa.sku_id"))),
                                        new ExpressionHashFunction(asList(e("ap.sku_id"))),
                                        new ExpressionPredicate(e("ap.sku_id = aa.sku_id")),
                                        DefaultRowMerger.DEFAULT,
                                        false,
                                        false),
                                queryResult.tableOperators.get(4),
                                new ExpressionHashFunction(asList(e("aa.attr1_id"))),
                                new ExpressionHashFunction(asList(e("a1.attr1_id"))),
                                new ExpressionPredicate(e("a1.attr1_id = aa.attr1_id")),
                                DefaultRowMerger.DEFAULT,
                                true,
                                false),
                        new ExpressionPredicate(e("ap.price_sales > 0"))),
                new ExpressionHashFunction(asList(e("s.art_id"))),
                new ExpressionHashFunction(asList(e("aa.art_id"))),
                new ExpressionPredicate(e("aa.art_id = s.art_id")),
                DefaultRowMerger.DEFAULT,
                true,
                false);

        //                System.err.println(expected.toString(1));
        //                System.out.println(queryResult.operator.toString(1));

        assertEquals(expected, queryResult.operator);

        Projection expectedProjection = new ObjectProjection(asList("sku_id"),
                asList(new ExpressionProjection(e("aa.sku_id"))));

        assertEquals(expectedProjection, queryResult.projection);
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
        TableAlias range = new TableAlias(article, of(session.getCatalogRegistry().getBuiltin().getName(), "range"), "r", new String[] {"Value"});
        new TableAlias(range, of("attribute1"), "a1", new String[] {"someId", "attr1_code"});

        //                                        System.out.println(article.printHierarchy(1));
        //                                        System.out.println(result.alias.printHierarchy(1));

        assertTrue("Alias hierarchy should be equal", article.isEqual(result.alias));
    }

    @Test
    public void test_single_table()
    {
        String query = "select s.id1, a.id2 from source s";
        QueryResult result = getQueryResult(query);

        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"a", "id1"});
        assertTrue(source.isEqual(result.alias));

        assertEquals(result.tableOperators.get(0), result.operator);
        assertEquals(new ObjectProjection(asList("id1", "id2"),
                asList(
                        new ExpressionProjection(e("s.id1")),
                        new ExpressionProjection(e("a.id2")))),
                result.projection);
    }

    @Test
    public void test_catalog_supported_predicates()
    {
        String query = "select s.id1, s.flag1 from source s where s.flag1 and s.flag2";

        MutableObject<Expression> catalogPredicate = new MutableObject<>();

        QueryResult result = getQueryResult(query, p ->
        {
            // flag1 is supported as filter
            AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(p.getPredicate());
            List<AnalyzePair> leftOvers = new ArrayList<>();
            for (AnalyzePair pair : analyzeResult.getPairs())
            {
                if (pair.getColumn("s", true).equals("flag1"))
                {
                    catalogPredicate.setValue(pair.getPredicate());
                }
                else
                {
                    leftOvers.add(pair);
                }
            }
            p.setPredicate(new AnalyzeResult(leftOvers).getPredicate());
        });

        assertEquals(e("s.flag1"), catalogPredicate.getValue());

        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"flag2", "flag1", "id1"});
        assertTrue(source.isEqual(result.alias));

        Operator expected = new FilterOperator(
                1,
                result.tableOperators.get(0),
                new ExpressionPredicate(e("s.flag2")));

        //        System.out.println(expected.toString(1));
        //        System.out.println(result.operator.toString(1));

        assertEquals(expected, result.operator);
        assertEquals(new ObjectProjection(asList("id1", "flag1"),
                asList(
                        new ExpressionProjection(e("s.id1")),
                        new ExpressionProjection(e("s.flag1")))),
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

        assertTrue("Alias hierarchy should be equal", source.isEqual(result.alias));

        Operator expected = new HashJoin(
                4,
                "",
                new FilterOperator(1, result.tableOperators.get(0), new ExpressionPredicate(e("s.id3 > 0"))),
                new FilterOperator(3, result.tableOperators.get(1), new ExpressionPredicate(e("a.active_flg and note_id > 0"))),
                new ExpressionHashFunction(asList(e("s.art_id"))),
                new ExpressionHashFunction(asList(e("a.art_id"))),
                new ExpressionPredicate(e("a.art_id = s.art_id")),
                DefaultRowMerger.DEFAULT,
                true,
                false);
        //
        //                        System.out.println(expected.toString(1));
        //                        System.err.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(asList("arr"),
                        asList(new ObjectProjection(asList("id1", "id2"),
                                asList(
                                        new ExpressionProjection(e("s.id1")),
                                        new ExpressionProjection(e("a.id2"))),
                                new FilterOperator(
                                        6,
                                        new ExpressionOperator(5, e("s")),
                                        new ExpressionPredicate(e("s.id4 > 0")))))),
                result.projection);
    }

    @Ignore
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
        assertTrue(source.isEqual(result.alias));

        Operator expected =
                // Correlated => nested loop
                new NestedLoopJoin(
                        0,
                        "",
                        result.tableOperators.get(0),
                        new HashJoin(
                                0,
                                "",
                                result.tableOperators.get(1),
                                result.tableOperators.get(2),
                                new ExpressionHashFunction(asList(e("a.art_id"))),
                                new ExpressionHashFunction(asList(e("aa.art_id"))),
                                new ExpressionPredicate(e("aa.art_id = a.art_id AND s.id")),
                                DefaultRowMerger.DEFAULT,
                                true,
                                false),
                        new ExpressionPredicate(e("a.art_id = s.art_id")),
                        DefaultRowMerger.DEFAULT,
                        true,
                        false);

        //                System.err.println(expected.toString(1));
        //                System.out.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(
                new ObjectProjection(asList("art_id"),
                        asList(new ExpressionProjection(e("s.art_id")))),
                result.projection);
    }

    //    @Ignore
    //    @Test
    //    public void test_correlated_manual()
    //    {
    //        String query = "SELECT s.art_id "
    //            + "FROM source s "
    //            + "INNER JOIN "
    //            + "["
    //            + "  article a"
    //            + "  INNER JOIN [articleAttribute] aa"
    //            + "    ON aa.art_id = a.art_id "
    //            + "    AND s.id "
    //            + "] a"
    //            + "  ON a.art_id = s.art_id";
    //
    //        Catalog c = new Catalog("TEST")
    //        {
    //            Random rnd = new Random();
    //
    //            @Override
    //            public Operator getScanOperator(int nodeId, String catalogAlias, TableAlias alias, List<TableOption> tableOptions)
    //            {
    //                QualifiedName qname = alias.getTable();
    //                if (qname.toString().equals("source"))
    //                {
    //                    return new CachingOperator(0, c -> IntStream.range(0, 1000).mapToObj(i -> Row.of(alias, i, new Object[] {rnd.nextBoolean(), rnd.nextInt(100)})).iterator());
    //                }
    //                else if (qname.toString().equals("article"))
    //                {
    //                    return new CachingOperator(0, c -> IntStream.range(0, 1000).mapToObj(i -> Row.of(alias, i, new Object[] {rnd.nextInt(1000)})).iterator());
    //                }
    //
    //                return new CachingOperator(0, c -> IntStream.range(0, 10000).mapToObj(i -> Row.of(alias, i, new Object[] {rnd.nextInt(1000)})).iterator());
    //            }
    //        };
    //
    //        session.setDefaultCatalog(c);
    //
    //        Pair<Operator, Projection> pair = null;
    //        for (int i = 0; i < 10; i++)
    //        {
    //            pair = OperatorBuilder.create(session, parser.parseSelect(query));
    //            ExecutionContext context = new ExecutionContext(session);
    //            Iterator<Row> it = pair.getKey().open(context);
    //            StopWatch sw = new StopWatch();
    //            sw.start();
    //            int count = 0;
    //            while (it.hasNext())
    //            {
    //                Row row = it.next();
    //                //            System.out.println(row);
    //                count++;
    //            }
    //            sw.stop();
    //            System.out.println("Time: " + sw.toString() + ", rows: " + count);
    //            System.out.println(FileUtils.byteCountToDisplaySize(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
    //        }
    //
    //        System.out.println(pair.getKey().toString(1));
    //
    //    }
}
