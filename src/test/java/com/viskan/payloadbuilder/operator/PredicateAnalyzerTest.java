package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.operator.PredicateAnalyzer.AnalyzeItem;
import com.viskan.payloadbuilder.operator.PredicateAnalyzer.AnalyzePair;
import com.viskan.payloadbuilder.operator.PredicateAnalyzer.AnalyzeResult;
import com.viskan.payloadbuilder.parser.Expression;
import com.viskan.payloadbuilder.parser.QueryParser;

import static com.viskan.payloadbuilder.utils.CollectionUtils.asSet;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;

import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

/** Test of {@link PredicateAnalyzer} */
public class PredicateAnalyzerTest extends Assert
{
    private final QueryParser parser = new QueryParser();

    @Test
    public void test_1()
    {
        Expression e;
        AnalyzeResult actual;

        e = e("a.art_id = s.art_id " +
            "  AND a.id = s.id + a.id2 " +
            "  AND a.active_flg ");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(e, actual.getPredicate());
        assertEquals(asList(pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))), actual.getEquiPairs("a", false));
        
        Pair<Expression, AnalyzeResult> pair = actual.extractPushdownPredicate("a", true);
        
        assertEquals(Pair.of(e("a.active_flg"), result(
                pair(asSet("a"), "id", e("a.id"), asSet("a", "s"), null, e("s.id + a.id2")),
                pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))
                )), pair);
        assertEquals(e("a.art_id = s.art_id AND a.id = s.id + a.id2"), pair.getValue().getPredicate());
    }

    @Test
    public void test()
    {
        Expression e;
        AnalyzeResult actual;

//        e = e("a.art_id = s.art_id AND a.active_flg");
//        actual = PredicateAnalyzer.analyze(e);
        
//        assertEquals(result(pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))), actual);
//        assertEquals(asList(pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))), actual.getEquiPairs("a", false));
//        assertNull(actual.extractPushdownPredicate("a", false).getKey());
//        assertEquals(Pair.of(e("a.art_id"), e("s.art_id")), actual.getEquiPairs("a", false).get(0).getExpressionPair("a", false));
//        assertEquals(Pair.of(e("s.art_id"), e("a.art_id")), actual.getEquiPairs("a", false).get(0).getExpressionPair("s", false));

//        assertEquals(e, actual.getPredicate());
        
        e = e("NOT (a.art_id = s.art_id)");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(e, actual.getPredicate());
        assertEquals(result(pair(asSet("a", "s"), null, e("NOT (a.art_id = s.art_id)"), emptySet(), null, null)), actual);
        assertNull(actual.extractPushdownPredicate("a", false).getKey());
        assertEquals(emptyList(), actual.getEquiPairs("a", false));
        assertEquals(e, actual.getPredicate());

        e = e("a.art_id = s.art_id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))), actual);
        assertEquals(asList(pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))), actual.getEquiPairs("a", false));
        assertNull(actual.extractPushdownPredicate("a", false).getKey());
        assertEquals(Pair.of(e("a.art_id"), e("s.art_id")), actual.getEquiPairs("a", false).get(0).getExpressionPair("a", false));
        assertEquals(Pair.of(e("s.art_id"), e("a.art_id")), actual.getEquiPairs("a", false).get(0).getExpressionPair("s", false));

        try
        {
            actual.getEquiPairs("a", true).get(0).getExpressionPair("b", true);
            fail("Should throw");
        }
        catch (IllegalArgumentException ex)
        {
            assertTrue(ex.getMessage().contains("No expressions could be found in this pair for alias b"));
        }

        e = e("art_id = s.art_id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(pair(asSet(""), "art_id", e("art_id"), asSet("s"), "art_id", e("s.art_id"))), actual);
        assertEquals(asList(pair(asSet(""), "art_id", e("art_id"), asSet("s"), "art_id", e("s.art_id"))), actual.getEquiPairs("a", true));
        assertNull(actual.extractPushdownPredicate("a", false).getKey());
        assertEquals(Pair.of(e("art_id"), e("s.art_id")), actual.getEquiPairs("a", true).get(0).getExpressionPair("a", true));
        assertEquals(Pair.of(e("s.art_id"), e("art_id")), actual.getEquiPairs("a", true).get(0).getExpressionPair("s", false));
        assertEquals(Pair.of(e("art_id"), e("s.art_id")), actual.getEquiPairs("a", true).get(0).getExpressionPair("s", true));

        e = e("s.art_id = art_id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(pair(asSet("s"), "art_id", e("s.art_id"), asSet(""), "art_id", e("art_id"))), actual);
        assertEquals(asList(pair(asSet("s"), "art_id", e("s.art_id"), asSet(""), "art_id", e("art_id"))), actual.getEquiPairs("s", true));
        assertNull(actual.extractPushdownPredicate("a", false).getKey());
        assertEquals(Pair.of(e("art_id"), e("s.art_id")), actual.getEquiPairs("a", true).get(0).getExpressionPair("a", true));
        //        assertEquals(Pair.of(e("art_id"), e("s.art_id")), actual.getEquiItems("b", true).get(0).getExpressionPair("b", true));
        assertEquals(Pair.of(e("s.art_id"), e("art_id")), actual.getEquiPairs("a", true).get(0).getExpressionPair("s", false));
        assertEquals(Pair.of(e("s.art_id"), e("art_id")), actual.getEquiPairs("a", true).get(0).getExpressionPair("s", true));

        e = e("art_id = s.art_id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(pair(asSet(""), "art_id", e("art_id"), asSet("s"), "art_id", e("s.art_id"))), actual);
        assertEquals(e, actual.getPredicate());

        e = e("s.art_id = a.art_id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(pair(asSet("s"), "art_id", e("s.art_id"), asSet("a"), "art_id", e("a.art_id"))), actual);
        assertEquals("art_id", actual.getPairs().get(0).getColumn("a", true));
        assertEquals("art_id", actual.getPairs().get(0).getColumn("s", true));

        e = e("a.art_id = s.art_id OR a.active_flg");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(pair(null, null, e("a.art_id = s.art_id OR a.active_flg"), null, null, null)), actual);
        assertNull(actual.getPairs().get(0).getColumn("a", true));

        e = e("a.art_id = s.art_id + a.id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(pair(asSet("a"), "art_id", e("a.art_id"), asSet("s", "a"), null, e("s.art_id + a.id"))), actual);
        assertNull(actual.getPairs().get(0).getColumn("s", true));

        e = e("a.art_id + s.id = s.art_id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(pair(asSet("a", "s"), null, e("a.art_id + s.id"), asSet("s"), "art_id", e("s.art_id"))), actual);
        assertEquals(emptyList(), actual.getEquiPairs("a", false));
        assertEquals(emptyList(), actual.getEquiPairs("s", false));

        e = e("a.art_id + a.idx_id = s.art_id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(pair(asSet("a"), null, e("a.art_id + a.idx_id"), asSet("s"), "art_id", e("s.art_id"))), actual);
        assertEquals(asList(pair(asSet("a"), null, e("a.art_id + a.idx_id"), asSet("s"), "art_id", e("s.art_id"))), actual.getEquiPairs("a", false));
        assertEquals(asList(pair(asSet("a"), null, e("a.art_id + a.idx_id"), asSet("s"), "art_id", e("s.art_id"))), actual.getEquiPairs("s", false));
        assertEquals(asList(pair(asSet("a"), null, e("a.art_id + a.idx_id"), asSet("s"), "art_id", e("s.art_id"))), actual.getEquiPairs("a", false));

        e = e("a.art_id = s.art_id AND s.sku_id = a.sku_id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(
                pair(asSet("s"), "sku_id", e("s.sku_id"), asSet("a"), "sku_id", e("a.sku_id")),
                pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))), actual);
        assertEquals(asList(
                pair(asSet("s"), "sku_id", e("s.sku_id"), asSet("a"), "sku_id", e("a.sku_id")),
                pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))),
                actual.getEquiPairs("s", false));
        assertEquals(asList(
                pair(asSet("s"), "sku_id", e("s.sku_id"), asSet("a"), "sku_id", e("a.sku_id")),
                pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))),
                actual.getEquiPairs("a", false));

        e = e("a.art_id > s.art_id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(pair(asSet("a", "s"), null, e("a.art_id > s.art_id"), emptySet(), null, null)), actual);
        assertEquals(e, actual.getPredicate());

        e = e("aa.art_id = a.art_id AND s.id");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(
                pair(asSet("s"), "id", e("s.id"), emptySet(), null, null),
                pair(asSet("aa"), "art_id", e("aa.art_id"), asSet("a"), "art_id", e("a.art_id"))), actual);
        assertEquals(e("s.id"), actual.extractPushdownPredicate("s", false).getKey());

        e = e("aa.art_id = a.art_id AND active_flg");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(
                pair(asSet(""), "active_flg", e("active_flg"), emptySet(), null, null),
                pair(asSet("aa"), "art_id", e("aa.art_id"), asSet("a"), "art_id", e("a.art_id"))), actual);
        
        Pair<Expression, AnalyzeResult> pair = actual.extractPushdownPredicate("s", true);
        
        assertEquals(e("active_flg"), pair.getKey());
        assertEquals(e("aa.art_id = a.art_id"), pair.getValue().getPredicate());

        e = e("a.active_flg = 1 AND aa.art_id = a.art_id AND s.id = aa.id2 AND 1 = a.internet_flg");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(e, actual.getPredicate());
        assertEquals(result(
                pair(emptySet(), null, e("1"), asSet("a"), "internet_flg", e("a.internet_flg")),
                pair(asSet("s"), "id", e("s.id"), asSet("aa"), "id2", e("aa.id2")),
                pair(asSet("aa"), "art_id", e("aa.art_id"), asSet("a"), "art_id", e("a.art_id")),
                pair(asSet("a"), "active_flg", e("a.active_flg"), emptySet(), null, e("1"))), actual);
        
        pair = actual.extractPushdownPredicate("a", true);
        
        assertEquals(e("1 = a.internet_flg AND a.active_flg = 1"), pair.getKey());
        assertEquals(e("aa.art_id = a.art_id AND s.id = aa.id2"), pair.getValue().getPredicate());

        e = e("aa.art_id = a.art_id AND (a.active_flg OR aa.active_flg)");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(
                pair(null, null, e("a.active_flg OR aa.active_flg"), null, null, null),
                pair(asSet("aa"), "art_id", e("aa.art_id"), asSet("a"), "art_id", e("a.art_id"))), actual);

        e = e("aa.art_id + aa.sku_id = a.art_id AND (a.active_flg OR aa.active_flg)");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(
                pair(null, null, e("a.active_flg OR aa.active_flg"), null, null, null),
                pair(asSet("aa"), null, e("aa.art_id + aa.sku_id"), asSet("a"), "art_id", e("a.art_id"))), actual);

        e = e("aa.art_id = a.sku_id + a.art_id AND (a.active_flg OR aa.active_flg)");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(result(
                pair(null, null, e("a.active_flg OR aa.active_flg"), null, null, null),
                pair(asSet("aa"), "art_id", e("aa.art_id"), asSet("a"), null, e("a.sku_id + a.art_id"))), actual);

        e = e("ap.art_id = aa.art_id AND ap.country_id = 0");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(e, actual.getPredicate());
        assertEquals(result(
                pair(asSet("ap"), "country_id", e("ap.country_id"), emptySet(), null, e("0")),
                pair(asSet("ap"), "art_id", e("ap.art_id"), asSet("aa"), "art_id", e("aa.art_id"))), actual);
        
        pair = actual.extractPushdownPredicate("ap", true);
        
        assertEquals(e("ap.country_id = 0"), pair.getKey());
        assertEquals(e("ap.art_id = aa.art_id"), pair.getValue().getPredicate());

        e = e("ap.art_id = aa.art_id AND 0 = ap.country_id");
        actual = PredicateAnalyzer.analyze(e);
        
        assertEquals(e, actual.getPredicate());
        assertEquals(result(
                pair(emptySet(), null, e("0"), asSet("ap"), "country_id", e("ap.country_id")),
                pair(asSet("ap"), "art_id", e("ap.art_id"), asSet("aa"), "art_id", e("aa.art_id"))), actual);
        
        assertEquals(asList(
                pair(emptySet(), null, e("0"), asSet("ap"), "country_id", e("ap.country_id")),
                pair(asSet("ap"), "art_id", e("ap.art_id"), asSet("aa"), "art_id", e("aa.art_id"))
                ), actual.getEquiPairs("ap", true));
        
        pair = actual.extractPushdownPredicate("ap", false);
        
        assertEquals(e("0 = ap.country_id"), pair.getKey());
        assertEquals(e("ap.art_id = aa.art_id"), pair.getValue().getPredicate());
        
        e = e("a.active_flg");
        actual = PredicateAnalyzer.analyze(e);
        assertEquals(e, actual.getPredicate());
        assertEquals(result(
                pair(asSet("a"), "active_flg", e("a.active_flg"), emptySet(), null, null)), actual);
        
        pair = actual.extractPushdownPredicate("a", false);
        
        assertEquals(e("a.active_flg"), pair.getKey());
        assertEquals(null, pair.getValue().getPredicate());
    }

    private AnalyzeResult result(AnalyzePair... pairs)
    {
        return new AnalyzeResult(asList(pairs));
    }

    private AnalyzePair pair(
            Set<String> leftAlias,
            String leftColumn,
            Expression leftExpression,
            Set<String> rightAlias,
            String rightColumn,
            Expression rightExpression)
    {
        return new AnalyzePair(
                new AnalyzeItem(leftExpression, leftAlias, leftColumn),
                new AnalyzeItem(rightExpression, rightAlias, rightColumn));
    }

    private Expression e(String expression)
    {
        return parser.parseExpression(expression);
    }
}
