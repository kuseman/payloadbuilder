package com.viskan.payloadbuilder.catalog.etmarticlecategoryhz;

import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.operator.JsonStringWriter;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.OperatorBuilder;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.operator.Projection;
import com.viskan.payloadbuilder.operator.Row;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.SelectStatement;

import static java.util.Arrays.asList;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

//@Ignore
public class HzTest extends Assert
{
    CatalogRegistry reg = new CatalogRegistry();
    HzCatalog c = new HzCatalog();
    QueryParser parser = new QueryParser();

    @Before
    public void setUp()
    {
        reg.registerCatalog("hz", c);
        reg.setDefaultCatalog(c);
    }
    
    @Test
    public void test()
    {
        //        String queryString = "select a.art_id from article a" ;
        //        String queryString = "select a.art_id, ARRAY(sku_id from aa) skus from article a inner join [articleAttribute] aa on aa.art_id = a.art_id AND aa.active_flg AND aa.internet_flg WHERE a.active_flg AND a.internet_flg";
        //        String queryString = "select a.art_id, ARRAY(OBJECT(a.sku_id, a.pluno, a.active_flg, a.internet_flg) FROM a) skus from range(15000, 15020) x inner join [articleAttribute] a on a.art_id = x.Value ";

        //        String queryString = "select ap.price_sales from range(0, 85000) x inner join articleAttribute aa on aa.art_id = x.Value inner join articlePrice ap on ap.art_id = aa.art_id and ap.sku_id = aa.sku_id";
        //        String queryString = "select aa.art_id from articleAttribute aa inner join [articlePrice] ap on ap.art_id = aa.art_id and ap.sku_id = aa.sku_id";
        //        "article.art_id": {},
        //        "article.art_no": {},
        //        "article.articleType": {},
        //        "articleName.art_desc": {},
        //        "articleName.art_desc2": {},
        //        "articleName.artname": {},
        //        "articleName.link_friendly_name": {},
        //        "articleName.brandName": {},
        //
        String queryString = "select a.art_id " +
            ",  a.art_no " +
            ",  a.articleType " +
            ",  an.art_desc " +
            ",  an.art_desc2 " +
            ",  an.link_friendly_name " +
            ",  an.brandName " +
            ",  a.unlimit_flg OR aa.any(x -> x.ab.any(y -> y.balance_disp > 0)) inStock" +
            ",  array" +
            "   (" +
            "     object" +
            "     (" +
            "       sku_id " +
            "     , art_id " +
            "     , attr1_id " +
            "     , attr2_id " +
            "     , attr3_id " +
            "     , extra_1 " +
            "     , extra_2 " +
            "     , extra_3 " +
            "     , supplierNo " +
            "     , a.unlimit_flg OR ab.any(x -> x.balance_disp > 0) inStock" +
            "     , object" +
            "       (" +
            "         a1.attr1_no " +
            "       , a1.attr1_code " +
            "       ) attribute1" +
            "     , object" +
            "       (" +
            "         a2.attr2_no " +
            "       , a2.attr2_code " +
            "       ) attribute2" +
            "     , object" +
            "       (" +
            "         a3.attr2_no " +
            "       , a3.attr3_code " +
            "       ) attribute3" +
            "     , object" +
            "       (" +
            "         ap.price_sales " +
            "       , ap.price_org " +
            "       , ap.vat " +
            "       , ap.club_id " +
            "       ) articlePrice" +
            "     )" +
            "     from aa " +
            "   ) articleAttributes " +
            ",  array" +
            "   (" +
            "     object" +
            "     ("+
            "       default_flg" +
            "     , object" +
            "       ("+
            "         cat_id " +
            "       , cch.categoryName catname" +
            "       , cch.linkFriendlyName link_friendly_name" +
            "       , object" +
            "         (" +
            "           h.level " +
            "         , h.level_3 " +
            "         , h.level_4 " +
            "         ) hierarchy " +
            "       ) category " +
            "     )" +
            "     from ac" +
            "   ) articleCategories" +
            ",  array" +
            "   (" +
            "     object" +
            "     (" +
            "       art_id " +
            "     , attr1_id " +
            "     , attr2_id " +
            "     , attr3_id " +
            "     , row_id " +
            "     , imagetype " +
            "     , object" +
            "       (" +
            "         image_width " +
            "       , image_height " +
            "       , media_type " +
            "       , coalesce(mn.linkFriendlyName, linkFriendlyName, filename) filename " +
            "       , isnull(mn.media_desc, media_desc) media_desc " +
            "       ) media " +
            "     )" +
            "   from aam " +
            "   ) articleAttributeMedias " +
            ",  array" +
            "   (" +
            "     object" +
            "     (" +
            "       propertykey_id" +
            "       , pk.propertykey_name" +
            "       , pk.propertykey_name_internal" +
            "       , array" +
            "         (" +
            "           object" +
            "           (" +
            "             propertyvalue_id " +
            "             , propertyvalue_internal " +
            "             , propertyvalue " +
            "           )" +
            "           from pv" +
            "         ) propertyValues " +
            "     )" +
            "     from ap" +
            "   ) propertyKeys " +
            "from article a with (batch_size=1000) " +
            "inner join [articleName] an " +
            "    on an.art_id = a.art_id " +
            "    and an.active_flg " +
            "    and an.lang_id = :lang_id " +
            "inner join " +
            "[" +
            "    articleAttribute aa " +
            "    inner join [articlePrice] ap " +
            "      on ap.art_id = aa.art_id " +
            "      and ap.sku_id = aa.sku_id " +
            "      and ap.country_id in (:country_id) " +
            "      and ap.club_id in (:club_id) " +
            "      and ap.active_flg " +
            "      and ap.min_qty = 1 " +
            "    inner join [attribute1] a1 " +
            "      on a1.attr1_id = aa.attr1_id " +
            "      and a1.lang_id = :lang_id " +
            "    inner join [attribute2] a2 " +
            "      on a2.attr2_id = aa.attr2_id " +
            "      and a2.lang_id = :lang_id " +
            "    inner join [attribute3] a3 " +
            "      on a3.attr3_id = aa.attr3_id " +
            "      and a3.lang_id = :lang_id " +
            "    left join " +
            "    [" +
            "      articleBalance ab " +
            "      inner join [stockhouse] sh " +
            "        on sh.stockhouseId = ab.stockhouseId " +
            "        and sh.webCountryIds in (:country_id) " +
            "        and sh.active_flg " +
            "        and sh.include_in_stock_balances_flg " +
            "      where (isnull(:checkBalances, false) or ab.balance_disp > 0) " +
            "    ] ab " +
            "      on ab.art_id = aa.art_id " +
            "      and ab.sku_id = aa.sku_id " +
            "] aa " +
            "    on aa.art_id = a.art_id " +
            "    and aa.active_flg " +
            "    and aa.internet_flg " +
            "left join " +
            "[" +
            "  articleCategory ac " +
            "  inner join [category] c " +
            "    on c.cat_id = ac.cat_id " +
            "    and c.active_flg " +
            "  inner join [hierarchy] h " +
            "    on h.cat_id = ac.cat_id " +
            "  inner join [contentCategoryHead] cch " +
            "    on cch.cat_id = ac.cat_id " +
            "    and cch.lang_id = :lang_id " +
            "  where ac.active_flg " +
            "] ac " +
            "    on ac.art_id = a.art_id " +
            "left join " +
            "[ " +
            "  articleAttributeMedia aam " +
            "  left join [mediaName] mn " +
            "    on mn.media_id = aam.media_id " +
            "    and mn.lang_id = :lang_id " +
            "  where aam.active_flg " +
            "] aam " +
            "    on aam.art_id = a.art_id " +
            "    and (:mediaRowIds IS NULL or :mediaRowIds IN (aam.row_id))" +
            "left join " +
            "[" +
            "  articleProperty ap " +
            "  inner join [propertyValue] pv " +
            "    on pv.propertyvalue_id = ap.propertyvalue_id " +
            "    and pv.lang_id = :lang_id " +
            "  inner join [propertyKey] pk " +
            "    on pk.propertykey_id = ap.propertykey_id " +
            "    and pk.lang_id = :lang_id " +
            "  where ap.usedForPresentation " +
            "  group by ap.art_id, ap.propertykey_id " +
            "] ap " +
            "    on ap.art_id = a.art_id " +
            "where (not isnull(:checkBalances, false) or aa.any(x -> x.ab.map(y -> y.balance_disp).sum() > 0)) " +
            "and (not isnull(:checkMedia, false) or aam.any(x -> true))" +
            "and a.active_flg " +
            "and a.internet_flg ";

        Map<String, Object> params = new HashMap<>();
        params.put("country_id", 0);
        params.put("club_id", asList(0));
        params.put("lang_id", 1);
//        params.put("checkBalances", true);
//        params.put("checkMedia", true);
//        params.put("mediaRowIds", asList(1010));
        query(queryString, params, false, 100);
    }
    
    private void query(
            String queryString,
            Map<String, Object> params,
            boolean print,
            int loopCount)
    {
        SelectStatement query = parser.parseQuery(reg, queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(reg, query);
        Operator op = pair.getKey();
        Projection pj = pair.getValue();

        System.out.println(op.toString(1));

        JsonStringWriter jsw = new JsonStringWriter();

        for (int i = 0; i < loopCount; i++)
        {

            StopWatch sw = new StopWatch();
            sw.start();
            OperatorContext ctx = new OperatorContext(params);
            Iterator<Row> it = op.open(ctx);
            int count = 0;
            long length = 0;
            while (it.hasNext())
            {
                Row row = it.next();
                pj.writeValue(jsw, ctx, row);
//                                                        System.out.println(
                String result = jsw.getAndReset();
                length += result.length();
                if (print)
                {
                    System.out.println(result);
                }
                
//                                                                );
                
                //
                //            System.out.println(row);
                count++;
//                break;
            }

            sw.stop();
            long memory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            System.out.println("Total time: " + sw.toString() + " row count: " + count + ", length: " + FileUtils.byteCountToDisplaySize(length) + ", mem: " + FileUtils.byteCountToDisplaySize(memory));
        }
    }
}
