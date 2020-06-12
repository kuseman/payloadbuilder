package com.viskan.payloadbuilder.provider.hazelcast;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.analyze.OperatorBuilder;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.operator.JsonStringWriter;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.operator.Projection;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.tree.Query;

import static java.util.Arrays.asList;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

public class HzTest extends Assert
{

    @Test
    public void test()
    {
        CatalogRegistry reg = new CatalogRegistry();
        HzCatalog c = new HzCatalog();
        reg.registerCatalog(c);
        reg.setDefaultCatalog(c);

        QueryParser parser = new QueryParser();
        //        String queryString = "select a.art_id from article a" ;
        //        String queryString = "select a.art_id, ARRAY(sku_id from aa) skus from article a inner join [articleAttribute] aa on aa.art_id = a.art_id AND aa.active_flg AND aa.internet_flg WHERE a.active_flg AND a.internet_flg";
        //        String queryString = "select a.art_id, ARRAY(OBJECT(a.sku_id, a.pluno, a.active_flg, a.internet_flg) FROM a) skus from range(15000, 15020) x inner join [articleAttribute] a on a.art_id = x.Value ";

        //        String queryString = "select ap.price_sales from range(0, 85000) x inner join articleAttribute aa on aa.art_id = x.Value inner join articlePrice ap on ap.art_id = aa.art_id and ap.sku_id = aa.sku_id";
        //        String queryString = "select aa.art_id from articleAttribute aa inner join [articlePrice] ap on ap.art_id = aa.art_id and ap.sku_id = aa.sku_id";
        //
        String queryString = "select a.art_id " +
            ",  a.internet_flg " +
            ",  array" +
            "   (" +
            "     object" +
            "     (" +
            "       sku_id " +
            "     , art_id " +
            "     , attr1_id " +
            "     , attr2_id " +
            "     , attr3_id " +
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
            "     FROM aa " +
            "   ) articleAttributes " +
            ",  array(object(cat_id, active_flg) FROM ac) cat_ids " +
            ",  array(row_id FROM aam) row_ids " +
            ",  array(propertykey_id FROM ap) propertykey_ids " +
            "from range(:from, :to) x " +
            //            "from source x " +
            "inner join [article] a " +
            "    on a.art_id = x.Value " +
            "    and a.active_flg " +
            "    and a.internet_flg " +
            //
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
            "] aa " +
            "    on aa.art_id = x.Value " +
            "    and aa.active_flg " +
            "    and aa.internet_flg " +
            "inner join [articleCategory] ac " +
            "    on ac.art_id = x.Value " +
            //            "    and ac.active_flg " +
            "left join [articleAttributeMedia] aam " +
            "    on aam.art_id = x.Value " +
            //            "    and ac.active_flg " +
            "left join " +
            "[" +
            "  articleProperty ap " +
            "  inner join [propertyValue] pv " +
            "    on pv.propertyvalue_id = ap.propertyvalue_id " +
            "    and pv.lang_id = :lang_id " +
            "  inner join [propertyKey] pk " +
            "    on pk.propertykey_id = ap.propertykey_id " +
            "    and pk.lang_id = :lang_id " +
            "] ap " +
            "    on ap.art_id = x.Value ";

        Query query = parser.parseQuery(reg, queryString);
        Pair<Operator, Projection> pair = OperatorBuilder.create(reg, query);
        Operator op = pair.getKey();
        Projection pj = pair.getValue();

        System.out.println(op.toString(1));

        JsonStringWriter jsw = new JsonStringWriter();

//        int total = 5614; // OJ
//        int total = 85000; // SS
        int total = 16000; // VNP
        int batchSize = 2500;
        int batches = (total / batchSize) + 1;
        Map<String, Object> params = new HashMap<>();
        params.put("country_id", 0);
        params.put("club_id", asList(0));
        params.put("lang_id", 1);

        for (int i = 0; i < 1000; i++)
        {
            StopWatch sw = new StopWatch();
            sw.start();
            OperatorContext ctx = new OperatorContext(params);
            int totalCount = 0;

            for (int j = 0; j < batches; j++)
            {
                params.put("from", j * batchSize);
                params.put("to", Math.min((j * batchSize) + batchSize, total));

                StopWatch sw1 = new StopWatch();
                sw1.start();
                Iterator<Row> it = op.open(ctx);
                int count = 0;
                while (it.hasNext())
                {
                    Row row = it.next();
                    pj.writeValue(jsw, ctx, row);
//                                System.out.println(
                    jsw.getAndReset();
//                                        );
                    //
                    //            System.out.println(row);
                    count++;
                }
                totalCount += count;

                sw1.split();
                long memory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
                System.out.println("Batch: " + j + " time: " + sw1.toString() + ", mem: " + FileUtils.byteCountToDisplaySize(memory) + ", row count: " + count + ", params: " + params);
            }
            sw.stop();
            System.out.println("Total time: " + sw.toString() + " row count: " + totalCount);
        }

    }
}
