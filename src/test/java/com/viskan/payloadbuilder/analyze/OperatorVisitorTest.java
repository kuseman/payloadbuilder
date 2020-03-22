package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.tree.Query;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

/** Test of {@link OperatorVisitor} */
public class OperatorVisitorTest extends Assert
{
    private final QueryParser parser = new QueryParser();
    private final CatalogRegistry catalogRegistry = new CatalogRegistry();

    @Test
    public void test_invalid_alias_hierarchy()
    {
        Query query = parser.parseQuery(catalogRegistry, "select a from tableA a inner join tableB a on a.id = a.id ");
        try
        {
            OperatorVisitor2.create(catalogRegistry, query);
            fail("Alias already exists in parent hierarchy");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("already exists in parent hierarchy"));
        }

        query = parser.parseQuery(catalogRegistry, "select a from tableA a inner join [tableB] b on b.id = a.id inner join [tableC] b on b.id = a.id");
        try
        {
            OperatorVisitor2.create(catalogRegistry, query);
            fail("Alias already exists as child");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("already exists as child"));
        }
    }

    //    @Test
    //    public void test_columns_collecting()
    //    {
    //        Query query = parser.parseQuery(catalogRegistry, 
    //                "select r.a1.attr1_code"
    //              + ",     art_id"
    //              + ",     idx_id"
    //              + ",     object"
    //              + "      ("
    //              + "        pluno, "
    //              + "        object "
    //              + "        ("
    //              + "          a1.rgb_code"
    //              + "        ) attribute1, "
    //              + "        object "
    //              + "        ("
    //              + "          a1.colorGroup "
    //              + "          where a1.group_flg "
    //              + "        ) attribute1Group "
    //              + "        from aa "
    //              + "        order by aa.internet_date_start"
    //              + "      ) obj "
    //              + ",     array"
    //              + "      ("
    //              + "        attr2_code "
    //              + "        from aa.map(aa -> aa.a2) "
    //              + "        where aa.ean13 != ''"
    //              + "      ) arr "
    //              + ",     array"
    //              + "      ("
    //              + "        art_id,"
    //              + "        note_id "
    //              + "        from aa.concat(aa.ap)"
    //              + "      ) arr2 "
    //              + "from article a "
    //              + "inner join "
    //              + "["
    //              + "  articleAttribute aa "
    //              + "  inner join [articlePrice] ap "
    //              + "    on ap.sku_id = aa.sku_id "
    //              + "    and ap.price_sales > 0"
    //              + "  inner join [attribute1] a1 "
    //              + "    on a1.attr1_id = aa.attr1_id "
    //              + "    and a1.lang_id = 1"
    //              + "  inner join [attribute2] a2 "
    //              + "    on a2.attr2_id = aa.attr2_id "
    //              + "    and a2.lang_id = 1"
    //              + "  inner join [attribute3] a3 "
    //              + "    on a3.attr3_id = aa.attr3_id "
    //              + "    and a3.lang_id = 1"
    //              + "  where ap.price_org > 0"
    //              + "  order by a2.attr2_no "
    //              + "] aa "
    //              + "  on aa.art_id = a.art_id "
    //              + "  and aa.active_flg"
    //              + "  and aa.internet_flg"
    //              + "  and a.articleType = 'regular'"
    //              + "inner join "
    //              + "["
    //              + "  articleProperty"
    //              + "  group by propertykey_id "
    //              + "] ap"
    //              + "  on ap.art_id = a.art_id "
    //              + "CROSS APPLY "
    //              + "["
    //              + "  range(10) r"
    //              + "  inner join [attribute1] a1 "
    //              + "    on a1.someId = r.Value"
    //              + "] r "
    //              + "where not a.add_on_flg "
    //              + "group by a.note_id "
    //              +" order by a.stamp_dat_cr");
    //       
    //       TableAlias actual = OperatorVisitor2.create(catalogRegistry, query);
    //       System.out.println(actual.printHierarchy(0));
    //       TableAlias article = new TableAlias(null, of("article"), "a", new String[] {"stamp_dat_cr", "art_id", "add_on_flg", "articleType", "note_id", "idx_id"});
    //       TableAlias articleAttribute = new TableAlias(article, of("articleAttribute"), "aa", new String[] { "internet_flg", "internet_date_start", "sku_id", "attr1_id", "art_id", "pluno", "active_flg", "ean13", "attr3_id", "note_id", "attr2_id" });
    //       new TableAlias(articleAttribute, of("articlePrice"), "ap", new String[] { "price_sales", "sku_id", "art_id", "price_org", "note_id" });
    //       new TableAlias(articleAttribute, of("attribute1"), "a1", new String[] { "colorGroup", "attr1_id", "rgb_code", "lang_id", "group_flg" });
    //       new TableAlias(articleAttribute, of("attribute2"), "a2", new String[] { "attr2_code", "lang_id", "attr2_no", "attr2_id" });
    //       new TableAlias(articleAttribute, of("attribute3"), "a3", new String[] { "lang_id", "attr3_id" });
    //       new TableAlias(article, of("articleProperty"), "ap", new String[] { "propertykey_id", "art_id" });
    //       TableAlias range = new TableAlias(article, of("DEFAULT", "range"), "r", new String[] { "Value" });
    //       new TableAlias(range, of("attribute1"), "a1", new String[] { "someId", "attr1_code" });
    //
    //       assertTrue("Alias hierarchy should be equal", article.isEqual(actual));
    //    }

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
                "select r.a1.attr1_code "
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
                    + "  on ab.artileBrandId = a.articleBrandId "
                    + "");

//        Query query = parser.parseQuery(catalogRegistry,
//                "select r.Value "
//                    + "from source s "
//                    + "outer apply range(10) r"
//                    + "");
        
        catalogRegistry.getDefault().setOperatorFactory(tableAlias -> new Operator()
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

        Operator operator = OperatorVisitor2.create(catalogRegistry, query);
        System.err.println(operator.toString(1));

        Iterator<Row> it = operator.open(new OperatorContext());
        while (it.hasNext())
        {
            Row row = it.next();
            System.out.println(row);
            System.out.println();
        }
    }
}
