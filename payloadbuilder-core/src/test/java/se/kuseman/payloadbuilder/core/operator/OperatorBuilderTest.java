package se.kuseman.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static se.kuseman.payloadbuilder.api.QualifiedName.of;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.TableAlias.TableAliasBuilder;
import se.kuseman.payloadbuilder.api.TableAlias.Type;
import se.kuseman.payloadbuilder.api.catalog.IAnalyzePair;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair;
import se.kuseman.payloadbuilder.core.parser.QualifiedReferenceExpression;
import se.kuseman.payloadbuilder.core.parser.QualifiedReferenceExpression.ResolvePath;
import se.kuseman.payloadbuilder.core.parser.Select;
import se.kuseman.payloadbuilder.core.parser.Select.For;
import se.kuseman.payloadbuilder.core.parser.SortItem;
import se.kuseman.payloadbuilder.core.parser.SubQueryExpression;

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
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Alias a already exists in scope."));
        }

        try
        {
            getQueryResult("select a from tableA a inner join tableB b on b.id = a.id inner join tableC b on b.id = a.id");
            fail("defined multiple times for parent");
        }
        catch (RuntimeException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Alias b already exists in scope."));
        }
    }

    @Test
    public void test_aggregating_select_items()
    {
        String query = "\r\n" + "select  *\r\n"
                       + ",       col2  \r\n"
                       + "from \r\n"
                       + "(\r\n"
                       + "    select  col2    \r\n"
                       + "    ,       col1\r\n"
                       + "    ,       *\r\n"
                       + "    ,       col3    \r\n"
                       + "    from \r\n"
                       + "    (\r\n"
                       + "        select  *\r\n"
                       + "        ,       col4\r\n"
                       + "        ,       col10 + col11 calc\r\n"
                       + "        ,       col5\r\n"
                       + "        ,       col6\r\n"
                       + "        from table\r\n"
                       + "    ) y\r\n"
                       + ") x";

        // col2
        // col1
        // *
        // col4
        // col10 + col11 calc
        // col5
        // col6
        // col3
        // col2
        //

        QueryResult queryResult = getQueryResult(query);
        assertEquals(new ComputedColumnsOperator(1, 0, queryResult.tableOperators.get(0), new ExpressionOrdinalValuesFactory(asList(en("col10 + col11")))), queryResult.operator);

        Projection expected = new RootProjection(asList("col2", "col1", "", "col4", "calc", "col5", "col6", "col3", "col2"),
                asList(new ExpressionProjection(en("col2")), new ExpressionProjection(en("col1")), new AsteriskProjection(new int[] { 2 }), new ExpressionProjection(en("col4")),
                        new ExpressionProjection(en("expr_2_0")), new ExpressionProjection(en("col5")), new ExpressionProjection(en("col6")), new ExpressionProjection(en("col3")),
                        new ExpressionProjection(en("col2"))));

        assertEquals(expected, queryResult.projection);
    }

    @Test
    public void test_batch_limit_operator()
    {
        String query = "select a.art_id from source s with (batch_limit=250) inner join article a on a.art_id = s.art_id";
        QueryResult queryResult = getQueryResult(query);

        Operator expected = new BatchRepeatOperator(4, 1,
                new HashJoin(3, "", new BatchLimitOperator(1, queryResult.tableOperators.get(0), e("250")), queryResult.tableOperators.get(1), new ExpressionHashFunction(asList(en("s.art_id"))),
                        new ExpressionHashFunction(asList(en("a.art_id"))), new ExpressionPredicate(en("a.art_id = s.art_id")), new DefaultTupleMerger(-1, 1, 2), false, false));

        // System.out.println(queryResult.operator.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, queryResult.operator);
    }

    @Test
    public void test_orderBy()
    {
        String query = "select a.art_id from article a order by a.art_id";
        QueryResult queryResult = getQueryResult(query);

        Operator expected = new OrderByOperator(1, queryResult.tableOperators.get(0), new ExpressionTupleComparator(asList(new SortItem(en("a.art_id"), Order.ASC, NullOrder.UNDEFINED, null))));

        // System.out.println(queryResult.operator.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, queryResult.operator);
    }

    @Test
    public void test_orderBy_with_calculated_columns()
    {
        String query = "select a.art_id, art_id * 10 newCol from article a order by newcol";
        QueryResult queryResult = getQueryResult(query);

        Operator expected = new OrderByOperator(2, new ComputedColumnsOperator(1, 0, queryResult.tableOperators.get(0), new ExpressionOrdinalValuesFactory(asList(en("art_id * 10")))),
                new ExpressionTupleComparator(
                        asList(new SortItem(new QualifiedReferenceExpression(QualifiedName.of("expr_0_0"), -1, new ResolvePath[] { new ResolvePath(-1, 0, emptyList(), 1_000_000) }, null), Order.ASC,
                                NullOrder.UNDEFINED, null))));

        // System.out.println(queryResult.operator.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, queryResult.operator);

        // Verify that projection is changed
        Projection expectedProjection = new RootProjection(asList("art_id", "newCol"), asList(new ExpressionProjection(en("a.art_id")),
                new ExpressionProjection(new QualifiedReferenceExpression(QualifiedName.of("expr_0_0"), -1, new ResolvePath[] { new ResolvePath(-1, 0, emptyList(), 1_000_000) }, null))));

        // System.err.println(expected.toString(1));
        // System.out.println(queryResult.operator.toString(1));

        assertEquals(expectedProjection, queryResult.projection);
    }

    @Test
    public void test_top()
    {
        String query = "select top 10 a.art_id from article a";
        QueryResult queryResult = getQueryResult(query);

        Operator expected = new TopOperator(1, queryResult.tableOperators.get(0), e("10"));

        // System.out.println(queryResult.operator.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, queryResult.operator);
    }

    @Test
    public void test_groupBy()
    {
        String query = "select a.art_id from article a group by a.art_id";
        QueryResult queryResult = getQueryResult(query);

        Operator expected = new GroupByOperator(1, queryResult.tableOperators.get(0), ofEntries(entry(0, asSet("art_id"))), new ExpressionOrdinalValuesFactory(asList(en("a.art_id"))), 1);

        // System.out.println(queryResult.operator.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, queryResult.operator);
    }

    @Test
    public void test_no_push_down_from_where_to_join_when_accessing_nested_alias()
    {
        String query = "select * " + "from source s "
                       + "inner join "
                       + "("
                       + "  select * "
                       + "  from article a "
                       + "  inner join articleBrand ab with(populate=true) "
                       + "    on ab.art_id = a.art_id"
                       + ") a with(populate=true) "
                       + "  on a.art_id = s.art_id "
                       + "where a.ab.active_flg";

        QueryResult queryResult = getQueryResult(query);

        Operator expected = new FilterOperator(5,
                new HashJoin(4, "INNER JOIN", queryResult.tableOperators.get(0),
                        new HashJoin(3, "INNER JOIN", queryResult.tableOperators.get(1), queryResult.tableOperators.get(2), new ExpressionHashFunction(asList(en("a.art_id"))),
                                new ExpressionHashFunction(asList(en("ab.art_id"))), new ExpressionPredicate(en("ab.art_id = a.art_id")), new DefaultTupleMerger(-1, 3, 2), true, false),
                        new ExpressionHashFunction(asList(en("s.art_id"))), new ExpressionHashFunction(asList(en("a.art_id"))), new ExpressionPredicate(en("a.art_id = s.art_id")),
                        new DefaultTupleMerger(-1, 1, 3), true, false),
                new ExpressionPredicate(en("a.ab.active_flg = true")));

        // System.out.println(queryResult.operator.toString(1));
        // System.out.println();
        // System.err.println(expected.toString(1));

        assertEquals(expected, queryResult.operator);
    }

    @Test
    public void test_no_push_down_from_left_join_on_predicate_accessing_previous_alias()
    {
        String query = "select raa.sku_id " + "from source s "
                       + "inner join article a "
                       + "  on a.art_id = s.art_id "
                       + "left join related_article raa "
                       + "  on raa.art_id = a.art_id "
                       + "  and a.articleType = 'type'";

        QueryResult queryResult = getQueryResult(query);

        Operator expected = new HashJoin(4, "LEFT JOIN",
                new HashJoin(2, "INNER JOIN", queryResult.tableOperators.get(0), queryResult.tableOperators.get(1), new ExpressionHashFunction(asList(en("s.art_id"))),
                        new ExpressionHashFunction(asList(en("a.art_id"))), new ExpressionPredicate(en("a.art_id = s.art_id")), new DefaultTupleMerger(-1, 1, 3), false, false),
                queryResult.tableOperators.get(2), new ExpressionHashFunction(asList(en("a.art_id"))), new ExpressionHashFunction(asList(en("raa.art_id"))),
                new ExpressionPredicate(en("raa.art_id = a.art_id AND a.articleType = 'type'")), new DefaultTupleMerger(-1, 2, 3), false, true);

        // System.out.println(queryResult.operator.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, queryResult.operator);
    }

    @Test
    public void test_push_down_from_where_to_join()
    {
        String query = "select * from source s inner join article a on a.art_id = s.art_id where a.active_flg";
        QueryResult queryResult = getQueryResult(query);

        Operator expected = new HashJoin(3, "INNER JOIN", queryResult.tableOperators.get(0),
                new FilterOperator(2, queryResult.tableOperators.get(1), new ExpressionPredicate(en("a.active_flg = true"))), new ExpressionHashFunction(asList(en("s.art_id"))),
                new ExpressionHashFunction(asList(en("a.art_id"))), new ExpressionPredicate(en("a.art_id = s.art_id")), new DefaultTupleMerger(-1, 1, 2), false, false);

        // System.out.println(queryResult.operator.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, queryResult.operator);
    }

    @Test
    public void test_push_down_from_where_to_join_when_left()
    {
        String query = "select * from source s left join article a on a.art_id = s.art_id where a.active_flg";
        QueryResult queryResult = getQueryResult(query);

        Operator expected = new HashJoin(3, "LEFT JOIN", queryResult.tableOperators.get(0),
                new FilterOperator(2, queryResult.tableOperators.get(1), new ExpressionPredicate(en("a.active_flg = true"))), new ExpressionHashFunction(asList(en("s.art_id"))),
                new ExpressionHashFunction(asList(en("a.art_id"))), new ExpressionPredicate(en("a.art_id = s.art_id")), new DefaultTupleMerger(-1, 1, 2), false, true);

        // System.out.println(queryResult.operator.toString(1));
        // System.err.println(expected.toString(1));

        assertEquals(expected, queryResult.operator);
    }

    @Test
    public void test_table_function()
    {
        String query = "select r.Value * r1.Value * r2.Value mul, r.Value r, r1.filter(x -> x.Value > 10).map(x -> x.Value) r1, r2.Value r2, (select Value from r1 for array) r1A "
                       + "from range(randomInt(100), randomInt(100) + 100) r "
                       + "inner join range(randomInt(100)) r1 with (populate=true) "
                       + "  on r1.Value <= r.Value "
                       + "inner join range(randomInt(100), randomInt(100) + 100) r2 "
                       + "  on r2.Value = r.Value";
        QueryResult queryResult = getQueryResult(query);

        TableFunctionInfo range = (TableFunctionInfo) session.getCatalogRegistry()
                .getSystemCatalog()
                .getFunction("range");

        TableAlias root = TableAliasBuilder.of(-1, Type.TABLE, QualifiedName.of("ROOT"), "ROOT")
                .children(asList(TableAliasBuilder.of(0, Type.FUNCTION, QualifiedName.of("range"), "r")
                        .tableMeta(range.getTableMeta()),
                        TableAliasBuilder.of(1, Type.FUNCTION, QualifiedName.of("range"), "r1")
                                .tableMeta(range.getTableMeta()),
                        TableAliasBuilder.of(2, Type.FUNCTION, QualifiedName.of("range"), "r2")
                                .tableMeta(range.getTableMeta())))
                .build();

        TableAlias r = root.getChildAliases()
                .get(0);
        TableAlias r1 = root.getChildAliases()
                .get(1);
        TableAlias r2 = root.getChildAliases()
                .get(2);

        Operator expected = new HashJoin(5, "INNER JOIN",
                new NestedLoopJoin(3, "INNER JOIN", new TableFunctionOperator(0, "", r, range, asList(e("randomInt(100)"), e("randomInt(100) + 100"))),
                        new CachingOperator(2, new TableFunctionOperator(1, "", r1, range, asList(e("randomInt(100)")))), new ExpressionPredicate(en("r1.Value <= r.Value")),
                        new DefaultTupleMerger(-1, 1, 3), true, false),
                new TableFunctionOperator(4, "", r2, range, asList(e("randomInt(100)"), e("randomInt(100) + 100"))), new ExpressionHashFunction(asList(en("r.Value"))),
                new ExpressionHashFunction(asList(en("r2.Value"))), new ExpressionPredicate(en("r2.Value = r.Value")), new DefaultTupleMerger(-1, 2, 3), false, false);

        // System.err.println(expected.toString(1));
        // System.out.println(queryResult.operator.toString(1));

        assertEquals(expected, queryResult.operator);

        TableAlias subQueryAlias = queryResult.aliases.get(queryResult.aliases.size() - 1);

        Projection expectedProjection = new RootProjection(asList("mul", "r", "r1", "r2", "r1A"),
                asList(new ExpressionProjection(en("r.Value * r1.Value * r2.Value")), new ExpressionProjection(en("r.Value")),
                        new ExpressionProjection(en("r1.filter(x -> x.Value > 10).map(x -> x.Value)")), new ExpressionProjection(en("r2.Value")),
                        new ExpressionProjection(new SubQueryExpression(new SubQueryExpressionOperator(7, subQueryAlias, queryResult.tableOperators.get(0)), new String[] { "Value" },
                                new ExpressionProjection[] { new ExpressionProjection(en("Value")) }, For.ARRAY))));

        // System.err.println(expected.toString(1));
        // System.out.println(queryResult.operator.toString(1));

        assertEquals(expectedProjection, queryResult.projection);
    }

    @Test
    public void test_mixed_populate()
    {
        String query = "select aa.sku_id " + "from source s "
                       + "inner join article a with(populate=true) "
                       + "  on a.art_id = s.art_id "
                       + "inner join "
                       + "("
                       + "  select * "
                       + "  from articleAttribute aa"
                       + "  inner join articlePrice ap"
                       + "    on ap.sku_id = aa.sku_id"
                       + "  inner join attribute1 a1 with(populate=true) "
                       + "    on a1.attr1_id = aa.attr1_id "
                       + "  where active_flg "
                       + "  and ap.price_sales > 0 "
                       + ") aa with(populate=true)"
                       + "  on aa.art_id = s.art_id"
                       + "   "
                       + "";

        QueryResult queryResult = getQueryResult(query);

        TableAlias root = TableAliasBuilder.of(-1, Type.ROOT, of("ROOT"), "ROOT")
                .children(asList(TableAliasBuilder.of(0, Type.TABLE, of("source"), "s"), TableAliasBuilder.of(1, Type.TABLE, of("article"), "a"),
                        TableAliasBuilder.of(2, Type.SUBQUERY, of("SubQuery"), "aa")
                                .children(asList(TableAliasBuilder.of(-1, Type.ROOT, of("ROOT"), "ROOT")
                                        .children(asList(TableAliasBuilder.of(3, Type.TABLE, of("articleAttribute"), "aa"), TableAliasBuilder.of(4, Type.TABLE, of("articlePrice"), "ap"),
                                                TableAliasBuilder.of(5, Type.TABLE, of("attribute1"), "a1")))))))
                .build();

        TableAlias source = root.getChildAliases()
                .get(0);

        // System.out.println(source.getParent().printHierarchy(0));
        // System.out.println(queryResult.alias.printHierarchy(0));

        assertTrue("Alias hierarchy should be equal", source.getParent()
                .isEqual(queryResult.alias));

        Operator expected = new HashJoin(10, "INNER JOIN",
                new HashJoin(2, "INNER JOIN", queryResult.tableOperators.get(0), queryResult.tableOperators.get(1), new ExpressionHashFunction(asList(en("s.art_id"))),
                        new ExpressionHashFunction(asList(en("a.art_id"))), new ExpressionPredicate(en("a.art_id = s.art_id")), new DefaultTupleMerger(-1, 1, 5), true, false),
                new HashJoin(9, "INNER JOIN",
                        new HashJoin(7, "INNER JOIN", new FilterOperator(4, queryResult.tableOperators.get(2), new ExpressionPredicate(en("active_flg = true"))),
                                new FilterOperator(6, queryResult.tableOperators.get(3), new ExpressionPredicate(en("ap.price_sales > 0"))), new ExpressionHashFunction(asList(en("aa.sku_id"))),
                                new ExpressionHashFunction(asList(en("ap.sku_id"))), new ExpressionPredicate(en("ap.sku_id = aa.sku_id")), new DefaultTupleMerger(-1, 4, 3), false, false),
                        queryResult.tableOperators.get(4), new ExpressionHashFunction(asList(en("aa.attr1_id"))), new ExpressionHashFunction(asList(en("a1.attr1_id"))),
                        new ExpressionPredicate(en("a1.attr1_id = aa.attr1_id")), new DefaultTupleMerger(-1, 5, 3), true, false),
                new ExpressionHashFunction(asList(en("s.art_id"))), new ExpressionHashFunction(asList(en("aa.art_id"))), new ExpressionPredicate(en("aa.art_id = s.art_id")),
                new DefaultTupleMerger(-1, 2, 5), true, false);

        // System.err.println(expected.toString(1));
        // System.out.println(queryResult.operator.toString(1));

        assertEquals(expected, queryResult.operator);

        Projection expectedProjection = new RootProjection(asList("sku_id"), asList(new ExpressionProjection(en("aa.sku_id"))));

        assertEquals(expectedProjection, queryResult.projection);
    }

    @Test
    public void test_columns_collecting()
    {
        String query = "select r.a1.attr1_code" + ", art_id"
                       + ", idx_id"
                       + ", "
                       + "  ("
                       + "    select "
                       + "    pluno, "
                       + "    ("
                       + "      select "
                       + "      aa.a1.rgb_code"
                       + "      for object"
                       + "    ) attribute1, "
                       + "    ("
                       + "      select "
                       + "      aa.a1.colorGroup "
                       + "      from open_rows(a1) "
                       + "      where aa.a1.group_flg "
                       + "      for object"
                       + "    ) attribute1Group "
                       + "    from open_rows(aa) "
                       + "    order by aa.internet_date_start"
                       + "    for object"
                       + "  ) obj, "
                       + "  ("
                       + "    select"
                       + "    attr2_code, "
                       + "    aa.map(x -> x) "
                       + "    from open_rows(aa.map(aa -> aa.a2)) "
                       + "    where aa.ean13 != ''"
                       + "    for array"
                       + "  ) arr, "
                       + "  ("
                       + "    select"
                       + "    art_id,"
                       + "    note_id "
                       + "    from open_rows(aa.ap)"
                       + "    for object"
                       + "  ) arr2 "
                       + "from article a "
                       + "inner join "
                       + "("
                       + "  select * "
                       + "  from articleAttribute aa "
                       + "  inner join articlePrice ap with(populate=true) "
                       + "    on ap.sku_id = aa.sku_id "
                       + "    and ap.price_sales > 0 "
                       + "  inner join attribute1 a1 with(populate=true) "
                       + "    on a1.attr1_id = aa.attr1_id "
                       + "    and a1.lang_id = 1 "
                       + "  inner join attribute2 a2 with(populate=true) "
                       + "    on a2.attr2_id = aa.attr2_id "
                       + "    and a2.lang_id = 1 "
                       + "  inner join attribute3 a3 with(populate=true) "
                       + "    on a3.attr3_id = aa.attr3_id "
                       + "    and a3.lang_id = 1 "
                       + "  where ap.price_org > 0"
                       + "  order by a2.attr2_no "
                       + ") aa with(populate=true)"
                       + "  on aa.art_id = a.art_id "
                       + "  and aa.active_flg "
                       + "  and aa.internet_flg "
                       + "inner join "
                       + "("
                       + "  select * "
                       + "  from articleProperty "
                       + "  group by propertykey_id "
                       + ") ap with(populate=true) "
                       + "  on ap.art_id = a.art_id "
                       + "cross apply "
                       + "("
                       + "  select * "
                       + "  from range(10) r "
                       + "  inner join attribute1 a1 with(populate=true)"
                       + "      on a1.someId = r.Value "
                       + ") r with(populate=true) "
                       + "where not a.add_on_flg and a.articleType = 'regular' "
                       + "group by a.note_id "
                       + "order by a.stamp_dat_cr";

        QueryResult result = getQueryResult(query);

        TableFunctionInfo rangeFunction = (TableFunctionInfo) session.getCatalogRegistry()
                .getSystemCatalog()
                .getFunction("range");

        TableAlias root = TableAliasBuilder.of(-1, TableAlias.Type.ROOT, QualifiedName.of("ROOT"), "ROOT")
                .children(asList(TableAliasBuilder.of(0, TableAlias.Type.TABLE, of("article"), "a"), TableAliasBuilder.of(1, TableAlias.Type.SUBQUERY, of("SubQuery"), "aa")
                        .children(asList(TableAliasBuilder.of(-1, TableAlias.Type.ROOT, QualifiedName.of("ROOT"), "ROOT")
                                .children(asList(TableAliasBuilder.of(2, TableAlias.Type.TABLE, of("articleAttribute"), "aa"), TableAliasBuilder.of(3, TableAlias.Type.TABLE, of("articlePrice"), "ap"),
                                        TableAliasBuilder.of(4, TableAlias.Type.TABLE, of("attribute1"), "a1"), TableAliasBuilder.of(5, TableAlias.Type.TABLE, of("attribute2"), "a2"),
                                        TableAliasBuilder.of(6, TableAlias.Type.TABLE, of("attribute3"), "a3"))))),

                        TableAliasBuilder.of(7, TableAlias.Type.SUBQUERY, of("SubQuery"), "ap")
                                .children(asList(TableAliasBuilder.of(-1, TableAlias.Type.ROOT, QualifiedName.of("ROOT"), "ROOT")
                                        .children(asList(TableAliasBuilder.of(8, TableAlias.Type.TABLE, of("articleProperty"), ""))))),
                        TableAliasBuilder.of(9, TableAlias.Type.SUBQUERY, of("SubQuery"), "r")
                                .children(asList(TableAliasBuilder.of(-1, TableAlias.Type.ROOT, QualifiedName.of("ROOT"), "ROOT")
                                        .children(asList(TableAliasBuilder.of(10, TableAlias.Type.FUNCTION, of("range"), "r")
                                                .tableMeta(rangeFunction.getTableMeta()), TableAliasBuilder.of(11, TableAlias.Type.TABLE, of("attribute1"), "a1")))))))
                .build();

        // System.out.println(root.printHierarchy(1));
        // System.out.println(result.alias.printHierarchy(1));

        assertTrue("Alias hierarchy should be equal", root.isEqual(result.alias));
    }

    @Test
    public void test_single_table()
    {
        String query = "select s.id1, s.id2 from source s";
        QueryResult result = getQueryResult(query);

        TableAlias root = TableAliasBuilder.of(-1, Type.ROOT, QualifiedName.of("ROOT"), "ROOT")
                .children(asList(TableAliasBuilder.of(0, TableAlias.Type.TABLE, QualifiedName.of("source"), "s")))
                .build();

        // System.out.println(root.printHierarchy(1));
        // System.out.println(result.alias.printHierarchy(1));

        assertTrue(root.isEqual(result.alias));

        assertEquals(result.tableOperators.get(0), result.operator);
        assertEquals(new RootProjection(asList("id1", "id2"), asList(new ExpressionProjection(en("s.id1")), new ExpressionProjection(en("s.id2")))), result.projection);
    }

    @Test
    public void test_catalog_supported_predicates()
    {
        String query = "select s.id1, s.flag1 from source s where s.flag1 and s.flag2";

        MutableObject<IExpression> catalogPredicate = new MutableObject<>();

        QueryResult result = getQueryResult(query, p ->
        {
            // flag1 is supported as filter
            Iterator<IAnalyzePair> it = p.iterator();
            while (it.hasNext())
            {
                AnalyzePair pair = (AnalyzePair) it.next();
                if (pair.getColumn("s")
                        .equals("flag1"))
                {
                    catalogPredicate.setValue(pair.getPredicate());
                    it.remove();
                }
            }
        }, null);

        assertEquals(en("s.flag1 = true"), catalogPredicate.getValue());

        Operator expected = new FilterOperator(1, result.tableOperators.get(0), new ExpressionPredicate(en("s.flag2 = true")));

        // System.out.println(expected.toString(1));
        // System.out.println(result.operator.toString(1));

        assertEquals(expected, result.operator);
        assertEquals(new RootProjection(asList("id1", "flag1"), asList(new ExpressionProjection(en("s.id1")), new ExpressionProjection(en("s.flag1")))), result.projection);
    }

    @Test
    public void test_catalog_supported_order_by()
    {
        String query = "select s.id1, s.flag1 from source s order by s.id1";

        MutableObject<List<ISortItem>> catalogOrderBy = new MutableObject<>();

        QueryResult result = getQueryResult(query, null, s ->
        {
            catalogOrderBy.setValue(new ArrayList<>(s));
            s.clear();
        });

        List<SortItem> expectedSortItems = asList(new SortItem(en("s.id1"), Order.ASC, NullOrder.UNDEFINED, null));
        assertEquals(expectedSortItems, catalogOrderBy.getValue());

        Operator expected = result.tableOperators.get(0);

        // System.out.println(expected.toString(1));
        // System.out.println(result.operator.toString(1));

        assertEquals(expected, result.operator);
        assertEquals(new RootProjection(asList("id1", "flag1"), asList(new ExpressionProjection(en("s.id1")), new ExpressionProjection(en("s.flag1")))), result.projection);
    }

    @Test
    public void test_pushdown_mixed_alias_aliasless()
    {
        String query = "select s.id from source s where s.flag and flag2";
        QueryResult result = getQueryResult(query);

        Operator expected = new FilterOperator(1, result.tableOperators.get(0), new ExpressionPredicate(en("s.flag = true and flag2 = true")));

        // System.out.println(expected.toString(1));
        // System.err.println(result.operator.toString(1));

        assertEquals(expected, result.operator);
    }

    @Test
    public void test_select_item_with_filter()
    {
        String query = "select (select s.id1, a.id2 from s where s.id4 > 0 for array) arr " + "from source s "
                       + "inner join "
                       + "( "
                       + "  select * "
                       + "  from article "
                       + "  where note_id > 0"
                       + ") a with(populate=true) "
                       + "  on a.art_id = s.art_id "
                       + "  and a.active_flg "
                       + "where s.id3 > 0 ";
        QueryResult result = getQueryResult(query);

        Operator expected = new HashJoin(4, "INNER JOIN", new FilterOperator(1, result.tableOperators.get(0), new ExpressionPredicate(en("s.id3 > 0"))),
                new FilterOperator(3, result.tableOperators.get(1), new ExpressionPredicate(en("note_id > 0 and a.active_flg = true"))), new ExpressionHashFunction(asList(en("s.art_id"))),
                new ExpressionHashFunction(asList(en("a.art_id"))), new ExpressionPredicate(en("a.art_id = s.art_id")), new DefaultTupleMerger(-1, 1, 2), true, false);

        // System.out.println(expected.toString(1));
        // System.out.println();
        // System.err.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        // The last alias added is the subquery one
        TableAlias subQueryAlias = result.aliases.get(result.aliases.size() - 1);

        Projection expectedProjection = new RootProjection(asList("arr"),
                asList(new ExpressionProjection(
                        new SubQueryExpression(new FilterOperator(7, new SubQueryExpressionOperator(6, subQueryAlias, result.tableOperators.get(2)), new ExpressionPredicate(en("s.id4 > 0"))),
                                new String[] { "id1", "id2" }, new Projection[] { new ExpressionProjection(en("s.id1")), new ExpressionProjection(en("a.id2")) }, Select.For.ARRAY))));

        assertEquals(expectedProjection, result.projection);
    }

    @Test
    public void test_correlated()
    {
        String query = "SELECT s.art_id " + "FROM source s "
                       + "INNER JOIN "
                       + "("
                       + "  select * "
                       + "  from article a"
                       + "  INNER JOIN articleAttribute aa with(populate=true)"
                       + "    ON aa.art_id = a.art_id "
                       + "    AND s.id "
                       + ") a with(populate=true)"
                       + "  ON a.art_id = s.art_id";

        QueryResult result = getQueryResult(query);

        Operator expected =
                // Correlated => nested loop
                new NestedLoopJoin(4, "INNER JOIN", result.tableOperators.get(0),
                        new HashJoin(3, "INNER JOIN", result.tableOperators.get(1), result.tableOperators.get(2), new ExpressionHashFunction(asList(en("a.art_id"))),
                                new ExpressionHashFunction(asList(en("aa.art_id"))), new ExpressionPredicate(en("aa.art_id = a.art_id AND s.id = true")), new DefaultTupleMerger(-1, 3, 2), true,
                                false),
                        new ExpressionPredicate(en("a.art_id = s.art_id")), new DefaultTupleMerger(-1, 1, 3), true, false);

        // System.err.println(expected.toString(1));
        // System.out.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(new RootProjection(asList("art_id"), asList(new ExpressionProjection(en("s.art_id")))), result.projection);
    }

    @Test
    public void test_not_correlated_when_a_sibling_alias_has_the_same_alias_as_a_parent()
    {
        String query = "SELECT s.art_id " + "FROM source s "
                       + "INNER JOIN "
                       + "("
                       + "  select * "
                       + "  from article a"
                       + "  INNER JOIN articleAttribute aa with(populate=true)"
                       + "    ON aa.art_id = a.art_id "
                       + "    AND s.id "
                       + ") a with(populate=true)"
                       + "  ON a.art_id = s.art_id";

        QueryResult result = getQueryResult(query);

        Operator expected =
                // Correlated => nested loop
                new NestedLoopJoin(4, "INNER JOIN", result.tableOperators.get(0),
                        new HashJoin(3, "INNER JOIN", result.tableOperators.get(1), result.tableOperators.get(2), new ExpressionHashFunction(asList(en("a.art_id"))),
                                new ExpressionHashFunction(asList(en("aa.art_id"))), new ExpressionPredicate(en("aa.art_id = a.art_id AND s.id = true")), new DefaultTupleMerger(-1, 3, 2), true,
                                false),
                        new ExpressionPredicate(en("a.art_id = s.art_id")), new DefaultTupleMerger(-1, 1, 3), true, false);

        // System.err.println(expected.toString(1));
        // System.out.println(result.operator.toString(1));

        assertEquals(expected, result.operator);

        assertEquals(new RootProjection(asList("art_id"), asList(new ExpressionProjection(en("s.art_id")))), result.projection);
    }
}
