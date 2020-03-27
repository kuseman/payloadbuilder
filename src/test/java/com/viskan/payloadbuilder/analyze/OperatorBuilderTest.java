package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.operator.CachingOperator;
import com.viskan.payloadbuilder.operator.ExpressionOperator;
import com.viskan.payloadbuilder.operator.ExpressionPredicate;
import com.viskan.payloadbuilder.operator.ExpressionProjection;
import com.viskan.payloadbuilder.operator.Filter;
import com.viskan.payloadbuilder.operator.JsonStringWriter;
import com.viskan.payloadbuilder.operator.NestedLoop;
import com.viskan.payloadbuilder.operator.ObjectProjection;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.operator.Projection;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;
import com.viskan.payloadbuilder.parser.tree.Query;

import static com.viskan.payloadbuilder.parser.tree.QualifiedName.of;
import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Collections.emptyIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/** Test of {@link OperatorVisitor} */
public class OperatorBuilderTest extends Assert
{
    private final QueryParser parser = new QueryParser();
    private final CatalogRegistry catalogRegistry = new CatalogRegistry();

    @Test
    public void test_invalid_alias_hierarchy()
    {
        catalogRegistry.getDefault().setOperatorFactory((a, b) -> c -> emptyIterator());
        Query query = parser.parseQuery(catalogRegistry, "select a from tableA a inner join tableB a on a.id = a.id ");
        try
        {
            OperatorBuilder.create(catalogRegistry, query);
            fail("Alias already exists in parent hierarchy");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("already exists in parent hierarchy"));
        }

        query = parser.parseQuery(catalogRegistry, "select a from tableA a inner join [tableB] b on b.id = a.id inner join [tableC] b on b.id = a.id");
        try
        {
            OperatorBuilder.create(catalogRegistry, query);
            fail("Alias already exists as child");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("already exists as child"));
        }
    }

    @Test
    public void test()
    {
        //        Query query = parser.parseQuery(catalogRegistry,
        //                "select r.a1.attr1_code "
        //              + "from article a "
        //              + "{"
        //              + "   inner join articleAttribute aa "
        //              + "   {"
        ////              + "     inner join articlePrice ap {} "
        ////              + "       on ap.sku_id = aa.sku_id "
        ////              + "       and ap.price_sales > 0"
        ////              + "     inner join attribute1 a1 {} "
        ////              + "       on a1.attr1_id = aa.attr1_id "
        ////              + "       and a1.lang_id = 1"
        ////              + "     inner join attribute2 a2 {} "
        ////              + "       on a2.attr2_id = aa.attr2_id "
        ////              + "       and a2.lang_id = 1"
        ////              + "     inner join attribute3 a3 {} "
        ////              + "       on a3.attr3_id = aa.attr3_id "
        ////              + "       and a3.lang_id = 1"
        ////              + "     where ap.price_org > 0"
        ////              + "     order by a2.attr2_no "
        //              + "   }"
        //              + "     on aa.art_id = a.art_id "
        //              + "     and aa.active_flg"
        //              + "     and aa.internet_flg"
        //              + "     and a.articleType = 'regular'"
        //              + "   inner join articleProperty ap "
        //              + "   {"
        ////              + "     group by propertykey_id "
        //              + "   }"
        //              + "     on ap.art_id = a.art_id "
        //              + "}"
        ////              + "where not a.add_on_flg "
        ////              + "group by a.note_id "
        ////              +" order by a.stamp_dat_cr"
        //              );

        Query query = parser.parseQuery(catalogRegistry,
                "select aa.a1.attr1_code, s.art_id + ab.articleBrandId col, array(sku_id, attr1_id, object(sku_id \"STOCK_\"\"KEEPING_UNIT\") from aa) sub "
                    + "from source s "
                    + "inner join [article] a "
                    + "  on a.art_id = s.art_id "
                    + "inner join "
                    + "["
                    + "  articleAttribute aa"
                    + "  inner join [articlePrice] ap"
                    + "    on ap.sku_id = aa.sku_id"
                    + "  inner join [attribute1] a1"
                    + "    on a1.attr1_id = aa.attr1_id"
                    + "] aa "
                    + "  on aa.art_id = s.art_id "
                    + "inner join articleBrand ab "
                    + "  on ab.articleBrandId = a.articleBrandId "
                    + "");

        //        Query query = parser.parseQuery(catalogRegistry,
        //                "select r.Value "
        //                    + "from source s "
        //                    + "outer apply range(10) r"
        //                    + "");

        /*
         * - Query should be built and compiled without the need of catalog registry ?
         * - Or
         *
         */

        catalogRegistry.getDefault().setOperatorFactory((tableName, tableAlias) -> new Operator()
        {
            @Override
            public Iterator<Row> open(OperatorContext c)
            {
                System.out.println(tableAlias + " " + Arrays.toString(tableAlias.getColumns()));

                int to = 50;
                return IntStream.range(0, new Random().nextInt(to) + 1)
                        .mapToObj(i -> Row.of(tableAlias, i, new Object[] {i, i, i}))
                        .iterator();
            }

            @Override
            public String toString()
            {
                return tableAlias.getTable().toString();
            }
        });

        Pair<Operator, Projection> pair = OperatorBuilder.create(catalogRegistry, query);
        Operator operator = pair.getLeft();
        Projection projection = pair.getRight();
        System.err.println(operator.toString(1));
        JsonStringWriter writer = new JsonStringWriter();

        OperatorContext context = new OperatorContext();
        Iterator<Row> it = operator.open(context);
        while (it.hasNext())
        {
            Row row = it.next();
            projection.writeValue(writer, context, row);
            System.out.println(writer.getAndReset());
            //            System.out.println(row);
            //            System.out.println();

        }
    }

    @Test
    public void test_columns_collecting()
    {
        String query = "select r.a1.attr1_code"
            + ",     art_id"
            + ",     idx_id"
            + ",     object"
            + "      ("
            + "        pluno, "
            + "        object "
            + "        ("
            + "          a1.rgb_code"
            + "        ) attribute1, "
            + "        object "
            + "        ("
            + "          a1.colorGroup "
            + "          from a1 "
            + "          where a1.group_flg "
            + "        ) attribute1Group "
            + "        from aa "
            + "        order by aa.internet_date_start"
            + "      ) obj "
            + ",     array"
            + "      ("
            + "        attr2_code "
            + "        from aa.map(aa -> aa.a2) "
            + "        where aa.ean13 != ''"
            + "      ) arr "
            + ",     array"
            + "      ("
            + "        art_id,"
            + "        note_id "
            + "        from aa.concat(aa.ap)"
            + "      ) arr2 "
            + "from article a "
            + "inner join "
            + "["
            + "  articleAttribute aa "
            + "  inner join [articlePrice] ap "
            + "    on ap.sku_id = aa.sku_id "
            + "    and ap.price_sales > 0"
            + "  inner join [attribute1] a1 "
            + "    on a1.attr1_id = aa.attr1_id "
            + "    and a1.lang_id = 1"
            + "  inner join [attribute2] a2 "
            + "    on a2.attr2_id = aa.attr2_id "
            + "    and a2.lang_id = 1"
            + "  inner join [attribute3] a3 "
            + "    on a3.attr3_id = aa.attr3_id "
            + "    and a3.lang_id = 1"
            + "  where ap.price_org > 0"
            + "  order by a2.attr2_no "
            + "] aa "
            + "  on aa.art_id = a.art_id "
            + "  and aa.active_flg"
            + "  and aa.internet_flg"
            + "  and a.articleType = 'regular'"
            + "inner join "
            + "["
            + "  articleProperty"
            + "  group by propertykey_id "
            + "] ap"
            + "  on ap.art_id = a.art_id "
            + "CROSS APPLY "
            + "["
            + "  range(10) r"
            + "  inner join [attribute1] a1 "
            + "    on a1.someId = r.Value"
            + "] r "
            + "where not a.add_on_flg "
            + "group by a.note_id "
            + " order by a.stamp_dat_cr";

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

        assertTrue("Alias hierarchy should be equal", article.isEqual(result.aliases.get(0)));
    }

    @Test
    public void test_nestedLoop()
    {
        String query = "select s.id1, a.id2 from source s inner join article a on s.art_id = a.art_id";
        QueryResult result = getQueryResult(query);

        // Assert aliaes
        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"art_id", "id1"});
        TableAlias article = new TableAlias(source, QualifiedName.of("article"), "a", new String[] {"art_id", "id2"});
        assertTrue(source.isEqual(result.aliases.get(0)));
        assertTrue(article.isEqual(result.aliases.get(1)));

        assertEquals(
                new NestedLoop(
                        result.tableOperator,
                        new CachingOperator(result.tableOperator),
                        new ExpressionPredicate(parser.parseExpression(catalogRegistry, "s.art_id = a.art_id")),
                        false),
                result.operator);

        assertEquals(
                new ObjectProjection(ofEntries(true,
                        entry("id1", new ExpressionProjection(parser.parseExpression(catalogRegistry, "s.id1"))),
                        entry("id2", new ExpressionProjection(parser.parseExpression(catalogRegistry, "a.id2"))))),
                result.projection);
    }

    @Test
    public void test_nestedLoop_populate()
    {
        String query = "select s.id1, a.id2 from source s inner join [article] a on s.art_id = a.art_id";
        QueryResult result = getQueryResult(query);

        // Assert aliaes
        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"art_id", "id1"});
        TableAlias article = new TableAlias(source, QualifiedName.of("article"), "a", new String[] {"art_id", "id2"});
        assertTrue(source.isEqual(result.aliases.get(0)));
        assertTrue(article.isEqual(result.aliases.get(1)));

        assertEquals(
                new NestedLoop(
                        result.tableOperator,
                        new CachingOperator(result.tableOperator),
                        new ExpressionPredicate(parser.parseExpression(catalogRegistry, "s.art_id = a.art_id")),
                        true),
                result.operator);

        assertEquals(
                new ObjectProjection(ofEntries(true,
                        entry("id1", new ExpressionProjection(parser.parseExpression(catalogRegistry, "s.id1"))),
                        entry("id2", new ExpressionProjection(parser.parseExpression(catalogRegistry, "a.id2"))))),
                result.projection);
    }

    @Test
    public void test_nestedLoop_with_filter()
    {
        String query = "select s.id1, a.id2 from source s inner join [article where note_id > 0] a on s.art_id = a.art_id where s.id3 > 0";
        QueryResult result = getQueryResult(query);

        // Assert aliaes
        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"art_id", "id3", "id1"});
        TableAlias article = new TableAlias(source, QualifiedName.of("article"), "a", new String[] {"art_id", "id2", "note_id"});
        assertTrue(source.isEqual(result.aliases.get(0)));
        assertTrue(article.isEqual(result.aliases.get(1)));

        assertEquals(
                new Filter(
                        new NestedLoop(
                                result.tableOperator,
                                new CachingOperator(new Filter(result.tableOperator, new ExpressionPredicate(parser.parseExpression(catalogRegistry, "note_id > 0")))),
                                new ExpressionPredicate(parser.parseExpression(catalogRegistry, "s.art_id = a.art_id")),
                                true),
                        new ExpressionPredicate(parser.parseExpression(catalogRegistry, "s.id3 > 0"))),
                result.operator);

        assertEquals(
                new ObjectProjection(ofEntries(true,
                        entry("id1", new ExpressionProjection(parser.parseExpression(catalogRegistry, "s.id1"))),
                        entry("id2", new ExpressionProjection(parser.parseExpression(catalogRegistry, "a.id2"))))),
                result.projection);
    }
    
    @Test
    public void test_select_item_with_filter()
    {
        String query = "select object(s.id1, a.id2 from s where s.id4 > 0) arr from source s inner join [article where note_id > 0] a on s.art_id = a.art_id where s.id3 > 0";
        QueryResult result = getQueryResult(query);

        // Assert aliaes
        TableAlias source = new TableAlias(null, QualifiedName.of("source"), "s", new String[] {"art_id", "id4", "id3", "id1"});
        TableAlias article = new TableAlias(source, QualifiedName.of("article"), "a", new String[] {"art_id", "id2", "note_id"});
        assertTrue(source.isEqual(result.aliases.get(0)));
        assertTrue(article.isEqual(result.aliases.get(1)));

        assertEquals(
                new Filter(
                        new NestedLoop(
                                result.tableOperator,
                                new CachingOperator(new Filter(result.tableOperator, new ExpressionPredicate(parser.parseExpression(catalogRegistry, "note_id > 0")))),
                                new ExpressionPredicate(parser.parseExpression(catalogRegistry, "s.art_id = a.art_id")),
                                true),
                        new ExpressionPredicate(parser.parseExpression(catalogRegistry, "s.id3 > 0"))),
                result.operator);

        assertEquals(
                new ObjectProjection(ofEntries(true,
                        entry("arr", new ObjectProjection(ofEntries(true,
                            entry("id1", new ExpressionProjection(parser.parseExpression(catalogRegistry, "s.id1"))),
                            entry("id2", new ExpressionProjection(parser.parseExpression(catalogRegistry, "a.id2")))),
                                new Filter(
                                        new ExpressionOperator(parser.parseExpression(catalogRegistry, "s")),
                                        new ExpressionPredicate(parser.parseExpression(catalogRegistry, "s.id4 > 0")))
                                ))
                        )),
                result.projection);
    }

    private QueryResult getQueryResult(String query)
    {
        Operator tableOperator = c -> emptyIterator();
        List<TableAlias> aliases = new ArrayList<>();
        catalogRegistry.getDefault().setOperatorFactory((qname, tableAlias) ->
        {
            aliases.add(tableAlias);
            return tableOperator;
        });

        Pair<Operator, Projection> pair = OperatorBuilder.create(catalogRegistry, parser.parseQuery(catalogRegistry, query));
        QueryResult result = new QueryResult();
        result.aliases = aliases;
        result.operator = pair.getLeft();
        result.projection = pair.getRight();
        result.tableOperator = tableOperator;
        return result;
    }

    static class QueryResult
    {
        Operator tableOperator;

        Operator operator;
        Projection projection;
        List<TableAlias> aliases;
    }
}
