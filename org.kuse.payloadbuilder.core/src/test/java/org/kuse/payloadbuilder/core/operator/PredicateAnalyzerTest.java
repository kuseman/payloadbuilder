package org.kuse.payloadbuilder.core.operator;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair.Type;
import org.kuse.payloadbuilder.core.parser.ComparisonExpression;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QueryParser;
import org.kuse.payloadbuilder.core.utils.CollectionUtils;

/** Test of {@link PredicateAnalyzer} */
public class PredicateAnalyzerTest extends Assert
{
    private final QueryParser parser = new QueryParser();
    
//    @Test
//    public void test_AnalyzeResult()
//    {
//        AnalyzeResult result = PredicateAnalyzer.analyze(e("a.art_id = s.art_id and sku_id = s.sku_id and active_flg and not a.internet_flg and a.value > 100"));
//        List<AnalyzePair> pairs = result.getEquiPairs("a");
//        System.out.println(pairs);
//        
//        Pair<Expression, AnalyzeResult> pair = result.extractPushdownPredicate("a");
//        System.out.println(pair.getKey());
//        System.out.println(pair.getValue().getPredicate());        
//    }
    
    @Test
    public void test_AnalyzePair()
    {
        /*
         * 
         * - Index: only supported is equals operator
         * - Pairs separated with AND
         *   Pair
         *     - Type (comparison-enum, in-expression, undefined)
         *     - Left-item
         *     - Right-item (null if undefined)
         *   Item
         *     - Alias (set if this item only references a single alias)
         *     - QualifiedName (set if this item references a qualified name, without Alias)
         *     - Expression (expression for this item)
         * 
         *  a.art_id = s.art_id
         *      Pair (Type: EQUALS)
         *      Left (Alias: a, QualifiedName: art_id, Expression: a.art_id)
         *      Right (Alias: s, QualifiedName: art_id, Expression: s.art_id)
         *     
         *  @timestamp > '......'
         *      Pair (Type: GREATER_THAN)
         *      Left (Alias: '', QualifiedName: @timestamp, Expression: @timestamp)
         *      Right (Alias: null, QualifiedName: null, Expression: '......')      
         *  
         *  !(a.art_id = s.art_id)
         *      Pair (Type: undefined)
         *      Left (Alias: null, QualifiedName: null, Expression: !(a.art_id = s.art_id))
         *      Right null     
         *  
         *  severity in (200, 300)
         *      Pair (Type: IN)
         *      Left (Alias: '', QualifiedName: severity, Expression: severity)
         *      Right (Alias: null, QualifiedName: null, Expression: severity in (200, 300))
         *  
         *  a.art_id in (s.art_id, -1)
         *      Pair (Type: IN)
         *      Left (Alias: a, QualifiedName: art_id, Expression: a.art_id)
         *      Right (Alias: null, QualifiedName: null, Expression: a.art_id in (s.art_id, -1))
         *  
         *  Use case: utilizing index in join
         *    - Get all pairs where type == EQUALS
         *    - See if all columns from index is present (getColumn on pair, default search on '' alias, only single qualified names)
         *  
         *  Use case: utilizing index in where
         *    - Same as for join
         *    - Find a single IN-pair referencing an index with that column, alias cannot be present in arguments
         *      Might be able in the future to find mix of IN,EQUALS and generate a cartesian list of values
         *  
         *  Use case: utilizing hash-join
         *    - Get all pairs where type == EQUALS
         *      and references inner alias (or empty) on one of the sides AND that alias is not present on opposite side   
         *    
         *  Use case: push down predicate
         *    - Get all pairs that only references (on both sides) provided alias. (getSingleAlias on pair.)
         *  
         */
        
        /*
         * from source a
         * inner join article s
         *   on a.art_id = art_id_rel
         * 
         */
        
        AnalyzePair p;
        
        p = PredicateAnalyzer.AnalyzePair.of(e(":val > s.art_id"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.GREATER_THAN, p.getComparisonType());
        assertEquals(e(":val > s.art_id"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertEquals("art_id", p.getColumn("s"));
        assertTrue(p.isPushdown("s"));
        assertFalse(p.isPushdown("b"));
        assertFalse(p.isEqui("s"));
        assertEquals(Pair.of(e("s.art_id"), e(":val")), p.getExpressionPair("s"));
        try
        {
            p.getExpressionPair("a");
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage().contains("No expressions could be found"));
        }
        
        p = PredicateAnalyzer.AnalyzePair.of(e("s.value + value2 > 10"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.GREATER_THAN, p.getComparisonType());
        assertEquals(e("s.value + value2 > 10"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("s"));
        assertTrue(p.isPushdown("s"));
        assertFalse(p.isPushdown("b"));
        assertEquals(Pair.of(e("s.value + value2"), e("10")), p.getExpressionPair("s"));
        
        p = PredicateAnalyzer.AnalyzePair.of(e("s.value + value2 + c.value > 10"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.GREATER_THAN, p.getComparisonType());
        assertEquals(e("s.value + value2 + c.value > 10"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("s"));
        assertFalse(p.isPushdown("s"));
        assertFalse(p.isPushdown("b"));
        
        p = PredicateAnalyzer.AnalyzePair.of(e("func(a.id) = func(s.id)"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("func(a.id) = func(s.id)"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("s"));
        assertFalse(p.isPushdown("s"));
        assertFalse(p.isPushdown("b"));
        assertTrue(p.isEqui("a"));
        assertTrue(p.isEqui("s"));
        assertFalse(p.isEqui("b"));
        assertEquals(Pair.of(e("func(a.id)"), e("func(s.id)")), p.getExpressionPair("a"));
        assertEquals(Pair.of(e("func(s.id)"), e("func(a.id)")), p.getExpressionPair("s"));
        
        p = PredicateAnalyzer.AnalyzePair.of(e("a.art_id = art_id_rel"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("a.art_id = art_id_rel"), p.getPredicate());
        assertEquals("art_id", p.getColumn("a"));
        assertEquals("art_id_rel", p.getColumn("s"));
        assertFalse(p.isPushdown("s"));
        assertTrue(p.isPushdown("a"));
        assertFalse(p.isPushdown("b"));
        
        p = PredicateAnalyzer.AnalyzePair.of(e("art_id = s.art_id_rel"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("art_id = s.art_id_rel"), p.getPredicate());
        assertEquals("art_id", p.getColumn("a"));
        assertEquals("art_id_rel", p.getColumn("s"));
        assertTrue(p.isPushdown("s"));
        assertFalse(p.isPushdown("a"));
        assertFalse(p.isPushdown("b"));
        
        p = PredicateAnalyzer.AnalyzePair.of(e("s.active"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("s.active = true"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertEquals("active", p.getColumn("s"));
        assertTrue(p.isPushdown("s"));
        assertFalse(p.isPushdown("b"));
        assertTrue(p.isEqui("s"));
        
        p = PredicateAnalyzer.AnalyzePair.of(e("active"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("active = true"), p.getPredicate());
        assertEquals("active", p.getColumn("b"));
        assertEquals("active", p.getColumn("s"));
        assertTrue(p.isPushdown("s"));
        assertTrue(p.isPushdown("b"));
        
        p = PredicateAnalyzer.AnalyzePair.of(e("s.id = s.id2"));
        assertEquals(Type.UNDEFINED, p.getType());
        assertNull(p.getComparisonType());
        assertEquals(e("s.id = s.id2"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("s"));
        assertEquals(CollectionUtils.asSet("s"), p.getLeft().aliases);
        assertTrue(p.isPushdown("s"));
        assertFalse(p.isPushdown("a"));
        
        p = PredicateAnalyzer.AnalyzePair.of(e("s.art_id > :val"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.GREATER_THAN, p.getComparisonType());
        assertEquals(e("s.art_id > :val"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertEquals("art_id", p.getColumn("s"));
        assertTrue(p.isPushdown("s"));
        
        p = PredicateAnalyzer.AnalyzePair.of(e("a.art_id = s.art_id + a.id"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("a.art_id = s.art_id + a.id"), p.getPredicate());
        assertNull(p.getColumn("s"));
        assertEquals("art_id", p.getColumn("a"));
        assertFalse(p.isPushdown("a"));
        assertFalse(p.isPushdown("s"));
        assertFalse(p.isPushdown("b"));
        assertFalse(p.isEqui("a"));
        assertFalse(p.isEqui("s"));
        assertFalse(p.isEqui("b"));
        
        p = PredicateAnalyzer.AnalyzePair.of(e("a.art_id = art_id_rel"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("a.art_id = art_id_rel"), p.getPredicate());
        assertEquals("art_id", p.getColumn("a"));
        assertEquals("art_id_rel", p.getColumn("s"));
        assertFalse(p.isEqui("a"));
        assertTrue(p.isEqui("s"));
        assertTrue(p.isEqui("b"));
        
        p = PredicateAnalyzer.AnalyzePair.of(e("a.b.art_id = s.art_id_rel"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("a.b.art_id = s.art_id_rel"), p.getPredicate());
        assertNull(p.getColumn("a"));
        assertEquals("art_id_rel", p.getColumn("s"));

        p = PredicateAnalyzer.AnalyzePair.of(e("a.art_id = s.d.art_id_rel"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("a.art_id = s.d.art_id_rel"), p.getPredicate());
        assertEquals("art_id", p.getColumn("a"));
        assertNull(p.getColumn("s"));
        
        p = PredicateAnalyzer.AnalyzePair.of(e("a.art_id >= s.art_id"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.GREATER_THAN_EQUAL, p.getComparisonType());
        assertEquals(e("a.art_id >= s.art_id"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertEquals("art_id", p.getColumn("a"));
        assertEquals("art_id", p.getColumn("s"));
        
        p = PredicateAnalyzer.AnalyzePair.of(e("severity in (200, 300)"));
        assertEquals(Type.IN, p.getType());
        assertNull(p.getComparisonType());
        assertEquals(e("severity in (200, 300)"), p.getPredicate());
        assertEquals("severity", p.getColumn("b"));
        assertEquals("severity", p.getColumn("a"));
        assertEquals("severity", p.getColumn("s"));

        p = PredicateAnalyzer.AnalyzePair.of(e("a.art_id in (s.id, b.id)"));
        assertEquals(Type.IN, p.getType());
        assertNull(p.getComparisonType());
        assertEquals(e("a.art_id in (s.id, b.id)"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertEquals("art_id", p.getColumn("a"));
        assertNull(p.getColumn("s"));

        p = PredicateAnalyzer.AnalyzePair.of(e(":val in (200, 300)"));
        assertEquals(Type.UNDEFINED, p.getType());
        assertNull(p.getComparisonType());
        assertEquals(e(":val in (200, 300)"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("a"));
        assertNull(p.getColumn("s"));
        
        p = PredicateAnalyzer.AnalyzePair.of(e("not (a.art_id > s.art_id)"));
        assertEquals(Type.UNDEFINED, p.getType());
        assertNull(p.getComparisonType());
        assertEquals(e("not (a.art_id > s.art_id)"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertNull(p.getColumn("a"));
        assertNull(p.getColumn("s"));
        
        p = PredicateAnalyzer.AnalyzePair.of(e("not a.flag"));
        assertEquals(Type.COMPARISION, p.getType());
        assertEquals(ComparisonExpression.Type.EQUAL, p.getComparisonType());
        assertEquals(e("a.flag = false"), p.getPredicate());
        assertNull(p.getColumn("b"));
        assertEquals("flag", p.getColumn("a"));
        assertNull(p.getColumn("s"));
        assertTrue(p.isPushdown("a"));
        assertFalse(p.isPushdown("s"));
    }
//    
//    @Test
//    public void test_1()
//    {
//        Expression e;
//        AnalyzeResult actual;
//
//        e = e("a.art_id = s.art_id " +
//            "  AND a.id = s.id + a.id2 " +
//            "  AND a.active_flg ");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(e, actual.getPredicate());
//        assertEquals(asList(pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))), actual.getEquiPairs("a", false));
//        
//        Pair<Expression, AnalyzeResult> pair = actual.extractPushdownPredicate("a", true);
//        
//        assertEquals(Pair.of(e("a.active_flg"), result(
//                pair(asSet("a"), "id", e("a.id"), asSet("a", "s"), null, e("s.id + a.id2")),
//                pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))
//                )), pair);
//        assertEquals(e("a.art_id = s.art_id AND a.id = s.id + a.id2"), pair.getValue().getPredicate());
//    }
//
//    @Test
//    public void test()
//    {
//        Expression e;
//        AnalyzeResult actual;
//
////        e = e("a.art_id = s.art_id AND a.active_flg");
////        actual = PredicateAnalyzer.analyze(e);
//        
////        assertEquals(result(pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))), actual);
////        assertEquals(asList(pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))), actual.getEquiPairs("a", false));
////        assertNull(actual.extractPushdownPredicate("a", false).getKey());
////        assertEquals(Pair.of(e("a.art_id"), e("s.art_id")), actual.getEquiPairs("a", false).get(0).getExpressionPair("a", false));
////        assertEquals(Pair.of(e("s.art_id"), e("a.art_id")), actual.getEquiPairs("a", false).get(0).getExpressionPair("s", false));
//
////        assertEquals(e, actual.getPredicate());
//        
//        e = e("NOT (a.art_id = s.art_id)");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(e, actual.getPredicate());
//        assertEquals(result(pair(asSet("a", "s"), null, e("NOT (a.art_id = s.art_id)"), emptySet(), null, null)), actual);
//        assertNull(actual.extractPushdownPredicate("a", false).getKey());
//        assertEquals(emptyList(), actual.getEquiPairs("a", false));
//        assertEquals(e, actual.getPredicate());
//
//        e = e("a.art_id = s.art_id");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(result(pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))), actual);
//        assertEquals(asList(pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))), actual.getEquiPairs("a", false));
//        assertNull(actual.extractPushdownPredicate("a", false).getKey());
//        assertEquals(Pair.of(e("a.art_id"), e("s.art_id")), actual.getEquiPairs("a", false).get(0).getExpressionPair("a", false));
//        assertEquals(Pair.of(e("s.art_id"), e("a.art_id")), actual.getEquiPairs("a", false).get(0).getExpressionPair("s", false));
//
//        try
//        {
//            actual.getEquiPairs("a", true).get(0).getExpressionPair("b", true);
//            fail("Should throw");
//        }
//        catch (IllegalArgumentException ex)
//        {
//            assertTrue(ex.getMessage().contains("No expressions could be found in this pair for alias b"));
//        }
//
//        e = e("art_id = s.art_id");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(result(pair(asSet(), "art_id", e("art_id"), asSet("s"), "art_id", e("s.art_id"))), actual);
//        assertEquals(asList(pair(asSet(), "art_id", e("art_id"), asSet("s"), "art_id", e("s.art_id"))), actual.getEquiPairs("a", true));
//        assertNull(actual.extractPushdownPredicate("a", false).getKey());
//        assertEquals(Pair.of(e("art_id"), e("s.art_id")), actual.getEquiPairs("a", true).get(0).getExpressionPair("a", true));
//        assertEquals(Pair.of(e("art_id"), e("s.art_id")), actual.getEquiPairs("a", true).get(0).getExpressionPair("s", false));
//        assertEquals(Pair.of(e("art_id"), e("s.art_id")), actual.getEquiPairs("a", true).get(0).getExpressionPair("s", true));
//
//        e = e("s.art_id = art_id");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(result(pair(asSet("s"), "art_id", e("s.art_id"), asSet(), "art_id", e("art_id"))), actual);
//        assertEquals(asList(pair(asSet("s"), "art_id", e("s.art_id"), asSet(), "art_id", e("art_id"))), actual.getEquiPairs("s", true));
//        assertNull(actual.extractPushdownPredicate("a", false).getKey());
//        assertEquals(Pair.of(e("art_id"), e("s.art_id")), actual.getEquiPairs("a", true).get(0).getExpressionPair("a", true));
//        //        assertEquals(Pair.of(e("art_id"), e("s.art_id")), actual.getEquiItems("b", true).get(0).getExpressionPair("b", true));
//        assertEquals(Pair.of(e("s.art_id"), e("art_id")), actual.getEquiPairs("a", true).get(0).getExpressionPair("s", false));
//        assertEquals(Pair.of(e("s.art_id"), e("art_id")), actual.getEquiPairs("a", true).get(0).getExpressionPair("s", true));
//
//        e = e("art_id = s.art_id");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(result(pair(asSet(), "art_id", e("art_id"), asSet("s"), "art_id", e("s.art_id"))), actual);
//        assertEquals(e, actual.getPredicate());
//
//        e = e("s.art_id = a.art_id");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(result(pair(asSet("s"), "art_id", e("s.art_id"), asSet("a"), "art_id", e("a.art_id"))), actual);
//        assertEquals("art_id", actual.getPairs().get(0).getColumn("a", true));
//        assertEquals("art_id", actual.getPairs().get(0).getColumn("s", true));
//
//        e = e("a.art_id = s.art_id OR a.active_flg");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(result(pair(null, null, e("a.art_id = s.art_id OR a.active_flg"), null, null, null)), actual);
//        assertNull(actual.getPairs().get(0).getColumn("a", true));
//
//        e = e("a.art_id = s.art_id + a.id");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(result(pair(asSet("a"), "art_id", e("a.art_id"), asSet("s", "a"), null, e("s.art_id + a.id"))), actual);
//        assertNull(actual.getPairs().get(0).getColumn("s", true));
//
//        e = e("a.art_id + s.id = s.art_id");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(result(pair(asSet("a", "s"), null, e("a.art_id + s.id"), asSet("s"), "art_id", e("s.art_id"))), actual);
//        assertEquals(emptyList(), actual.getEquiPairs("a", false));
//        assertEquals(emptyList(), actual.getEquiPairs("s", false));
//
//        e = e("a.art_id + a.idx_id = s.art_id");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(result(pair(asSet("a"), null, e("a.art_id + a.idx_id"), asSet("s"), "art_id", e("s.art_id"))), actual);
//        assertEquals(asList(pair(asSet("a"), null, e("a.art_id + a.idx_id"), asSet("s"), "art_id", e("s.art_id"))), actual.getEquiPairs("a", false));
//        assertEquals(asList(pair(asSet("a"), null, e("a.art_id + a.idx_id"), asSet("s"), "art_id", e("s.art_id"))), actual.getEquiPairs("s", false));
//        assertEquals(asList(pair(asSet("a"), null, e("a.art_id + a.idx_id"), asSet("s"), "art_id", e("s.art_id"))), actual.getEquiPairs("a", false));
//
//        e = e("a.art_id = s.art_id AND s.sku_id = a.sku_id");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(result(
//                pair(asSet("s"), "sku_id", e("s.sku_id"), asSet("a"), "sku_id", e("a.sku_id")),
//                pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))), actual);
//        assertEquals(asList(
//                pair(asSet("s"), "sku_id", e("s.sku_id"), asSet("a"), "sku_id", e("a.sku_id")),
//                pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))),
//                actual.getEquiPairs("s", false));
//        assertEquals(asList(
//                pair(asSet("s"), "sku_id", e("s.sku_id"), asSet("a"), "sku_id", e("a.sku_id")),
//                pair(asSet("a"), "art_id", e("a.art_id"), asSet("s"), "art_id", e("s.art_id"))),
//                actual.getEquiPairs("a", false));
//
//        e = e("a.art_id > s.art_id");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(result(pair(asSet("a", "s"), null, e("a.art_id > s.art_id"), emptySet(), null, null)), actual);
//        assertEquals(e, actual.getPredicate());
//
//        e = e("aa.art_id = a.art_id AND s.id");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(result(
//                pair(asSet("s"), "id", e("s.id"), emptySet(), null, null),
//                pair(asSet("aa"), "art_id", e("aa.art_id"), asSet("a"), "art_id", e("a.art_id"))), actual);
//        assertEquals(e("s.id"), actual.extractPushdownPredicate("s", false).getKey());
//
//        e = e("aa.art_id = a.art_id AND active_flg");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(result(
//                pair(asSet(), "active_flg", e("active_flg"), emptySet(), null, null),
//                pair(asSet("aa"), "art_id", e("aa.art_id"), asSet("a"), "art_id", e("a.art_id"))), actual);
//        
//        Pair<Expression, AnalyzeResult> pair = actual.extractPushdownPredicate("s", true);
//        
//        assertEquals(e("active_flg"), pair.getKey());
//        assertEquals(e("aa.art_id = a.art_id"), pair.getValue().getPredicate());
//
//        e = e("a.active_flg = 1 AND aa.art_id = a.art_id AND s.id = aa.id2 AND 1 = a.internet_flg");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(e, actual.getPredicate());
//        assertEquals(result(
//                pair(emptySet(), null, e("1"), asSet("a"), "internet_flg", e("a.internet_flg")),
//                pair(asSet("s"), "id", e("s.id"), asSet("aa"), "id2", e("aa.id2")),
//                pair(asSet("aa"), "art_id", e("aa.art_id"), asSet("a"), "art_id", e("a.art_id")),
//                pair(asSet("a"), "active_flg", e("a.active_flg"), emptySet(), null, e("1"))), actual);
//        
//        pair = actual.extractPushdownPredicate("a", true);
//        
//        assertEquals(e("1 = a.internet_flg AND a.active_flg = 1"), pair.getKey());
//        assertEquals(e("aa.art_id = a.art_id AND s.id = aa.id2"), pair.getValue().getPredicate());
//
//        e = e("aa.art_id = a.art_id AND (a.active_flg OR aa.active_flg)");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(result(
//                pair(null, null, e("a.active_flg OR aa.active_flg"), null, null, null),
//                pair(asSet("aa"), "art_id", e("aa.art_id"), asSet("a"), "art_id", e("a.art_id"))), actual);
//
//        e = e("aa.art_id + aa.sku_id = a.art_id AND (a.active_flg OR aa.active_flg)");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(result(
//                pair(null, null, e("a.active_flg OR aa.active_flg"), null, null, null),
//                pair(asSet("aa"), null, e("aa.art_id + aa.sku_id"), asSet("a"), "art_id", e("a.art_id"))), actual);
//
//        e = e("aa.art_id = a.sku_id + a.art_id AND (a.active_flg OR aa.active_flg)");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(result(
//                pair(null, null, e("a.active_flg OR aa.active_flg"), null, null, null),
//                pair(asSet("aa"), "art_id", e("aa.art_id"), asSet("a"), null, e("a.sku_id + a.art_id"))), actual);
//
//        e = e("ap.art_id = aa.art_id AND ap.country_id = 0");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(e, actual.getPredicate());
//        assertEquals(result(
//                pair(asSet("ap"), "country_id", e("ap.country_id"), emptySet(), null, e("0")),
//                pair(asSet("ap"), "art_id", e("ap.art_id"), asSet("aa"), "art_id", e("aa.art_id"))), actual);
//        
//        pair = actual.extractPushdownPredicate("ap", true);
//        
//        assertEquals(e("ap.country_id = 0"), pair.getKey());
//        assertEquals(e("ap.art_id = aa.art_id"), pair.getValue().getPredicate());
//
//        e = e("ap.art_id = aa.art_id AND 0 = ap.country_id");
//        actual = PredicateAnalyzer.analyze(e);
//        
//        assertEquals(e, actual.getPredicate());
//        assertEquals(result(
//                pair(emptySet(), null, e("0"), asSet("ap"), "country_id", e("ap.country_id")),
//                pair(asSet("ap"), "art_id", e("ap.art_id"), asSet("aa"), "art_id", e("aa.art_id"))), actual);
//        
//        assertEquals(asList(
//                pair(emptySet(), null, e("0"), asSet("ap"), "country_id", e("ap.country_id")),
//                pair(asSet("ap"), "art_id", e("ap.art_id"), asSet("aa"), "art_id", e("aa.art_id"))
//                ), actual.getEquiPairs("ap", true));
//        
//        pair = actual.extractPushdownPredicate("ap", false);
//        
//        assertEquals(e("0 = ap.country_id"), pair.getKey());
//        assertEquals(e("ap.art_id = aa.art_id"), pair.getValue().getPredicate());
//        
//        e = e("a.active_flg");
//        actual = PredicateAnalyzer.analyze(e);
//        assertEquals(e, actual.getPredicate());
//        assertEquals(result(
//                pair(asSet("a"), "active_flg", e("a.active_flg"), emptySet(), null, null)), actual);
//        
//        pair = actual.extractPushdownPredicate("a", false);
//        
//        assertEquals(e("a.active_flg"), pair.getKey());
//        assertEquals(null, pair.getValue().getPredicate());
//    }
//
//    private AnalyzeResult result(AnalyzePair... pairs)
//    {
//        return new AnalyzeResult(asList(pairs));
//    }
//
//    private AnalyzePair pair(
//            Set<String> leftAlias,
//            String leftColumn,
//            Expression leftExpression,
//            Set<String> rightAlias,
//            String rightColumn,
//            Expression rightExpression)
//    {
//        return new AnalyzePair(
//                new AnalyzeItem(leftExpression, leftAlias, leftColumn),
//                new AnalyzeItem(rightExpression, rightAlias, rightColumn));
//    }

    private Expression e(String expression)
    {
        return parser.parseExpression(expression);
    }
}
