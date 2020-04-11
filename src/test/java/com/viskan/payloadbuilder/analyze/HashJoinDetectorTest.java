package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/** Test of {@link HashJoinDetector} */
public class HashJoinDetectorTest extends Assert
{
    private final QueryParser parser = new QueryParser();
    private final CatalogRegistry catalogRegistry = new CatalogRegistry();
    
    @Test
    public void test()
    {
        Expression e;
        Pair<List<Expression>, List<Expression>> actual;

        e = e("a.art_id = s.art_id");
        actual = HashJoinDetector.detect(e, "a");
        assertEquals(Pair.of(asList(e("a.art_id")), asList(e("s.art_id"))), actual);

        e = e("a.art_id = s.art_id");
        actual = HashJoinDetector.detect(e, "aa");
        assertEquals(Pair.of(emptyList(), emptyList()), actual);

        e = e("a.art_id = s.art_id OR a.active_flg");
        actual = HashJoinDetector.detect(e, "a");
        assertEquals(Pair.of(emptyList(), emptyList()), actual);

        e = e("a.art_id = s.art_id + a.id");
        actual = HashJoinDetector.detect(e, "a");
        assertEquals(Pair.of(emptyList(), emptyList()), actual);

        e = e("a.art_id + s.id = s.art_id");
        actual = HashJoinDetector.detect(e, "a");
        assertEquals(Pair.of(emptyList(), emptyList()), actual);

        e = e("a.art_id = s.art_id AND s.sku_id = a.sku_id");
        actual = HashJoinDetector.detect(e, "a");
        assertEquals(Pair.of(asList(e("a.art_id"), e("a.sku_id")), asList(e("s.art_id"), e("s.sku_id"))), actual);
        
        e = e("a.art_id > s.art_id");
        actual = HashJoinDetector.detect(e, "a");
        assertEquals(Pair.of(emptyList(), emptyList()), actual);
        
        e = e("aa.art_id = a.art_id AND s.id");
        actual = HashJoinDetector.detect(e, "aa");
        assertEquals(Pair.of(asList(e("aa.art_id")), asList(e("a.art_id"))), actual);
        
        e = e("aa.art_id = a.art_id AND s.id = aa.id2");
        actual = HashJoinDetector.detect(e, "aa");
        assertEquals(Pair.of(asList(e("aa.art_id"), e("aa.id2")), asList(e("a.art_id"), e("s.id"))), actual);
        
        e = e("aa.art_id = a.art_id AND (a.active_flg OR aa.active_flg)");
        actual = HashJoinDetector.detect(e, "aa");
        assertEquals(Pair.of(asList(e("aa.art_id")), asList(e("a.art_id"))), actual);

        e = e("aa.art_id = a.art_id AND (a.active_flg OR aa.active_flg)");
        actual = HashJoinDetector.detect(e, "aa");
        assertEquals(Pair.of(asList(e("aa.art_id")), asList(e("a.art_id"))), actual);
    }
    
    private Expression e(String expression)
    {
        return parser.parseExpression(catalogRegistry, expression);
    }
}
