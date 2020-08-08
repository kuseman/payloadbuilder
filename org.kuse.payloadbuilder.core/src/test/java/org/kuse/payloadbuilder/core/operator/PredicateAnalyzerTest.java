package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.kuse.payloadbuilder.core.parser.QualifiedName.of;
import static org.kuse.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzeItem;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair.Type;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzeResult;
import org.kuse.payloadbuilder.core.parser.ComparisonExpression;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QueryParser;
import org.kuse.payloadbuilder.core.utils.CollectionUtils;

/** Test of {@link PredicateAnalyzer} */
public class PredicateAnalyzerTest extends Assert
{
    private final QueryParser parser = new QueryParser();

    @Test
    public void test_AnalyzeResult()
    {
        Pair<List<AnalyzePair>, AnalyzeResult> pair;
        AnalyzeResult result;
        result = PredicateAnalyzer.analyze(null);
        assertEquals(result, AnalyzeResult.EMPTY);
        assertNull(result.getPredicate());
        pair = result.extractPushdownPairs("a");
        assertEquals(emptyList(), pair.getLeft());
        assertEquals(AnalyzeResult.EMPTY, pair.getValue());
        
        result = PredicateAnalyzer.analyze(e("(a.flag)"));
        assertEquals(
                result(
                        pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.flag"), asSet("a"), of("flag"), e("true"), emptySet(), null)),
                result);
        assertEquals(e("(a.flag = true)"), result.getPredicate());
        pair = result.extractPushdownPairs("a");
        assertEquals(asList(
                pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.flag"), asSet("a"), of("flag"), e("true"), emptySet(), null)
                ),
                pair.getLeft());
        assertEquals(AnalyzeResult.EMPTY, pair.getValue());

        result = PredicateAnalyzer.analyze(e("a.art_id = s.art_id"));
        assertEquals(
                result(
                        pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.art_id"), asSet("a"), of("art_id"), e("s.art_id"), asSet("s"), of("art_id"))),
                result);
        assertEquals(e("a.art_id = s.art_id"), result.getPredicate());
        pair = result.extractPushdownPairs("a");
        assertEquals(emptyList(), pair.getLeft());
        assertEquals(result, pair.getValue());

        result = PredicateAnalyzer.analyze(e("a.art_id = s.art_id and sku_id = s.sku_id and active_flg and not a.internet_flg and a.value > 100"));

        assertEquals(
                result(
                        pair(Type.COMPARISION, ComparisonExpression.Type.GREATER_THAN, e("a.value"), asSet("a"), of("value"), e("100"), asSet(), null),
                        pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.internet_flg"), asSet("a"), of("internet_flg"), e("false"), asSet(), null),
                        pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("active_flg"), asSet(""), of("active_flg"), e("true"), asSet(), null),
                        pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("sku_id"), asSet(""), of("sku_id"), e("s.sku_id"), asSet("s"), of("sku_id")),
                        pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.art_id"), asSet("a"), of("art_id"), e("s.art_id"), asSet("s"), of("art_id"))),
                result);
        assertEquals(asList(
                pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.internet_flg"), asSet("a"), of("internet_flg"), e("false"), asSet(), null),
                pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("active_flg"), asSet(""), of("active_flg"), e("true"), asSet(), null),
                pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("sku_id"), asSet(""), of("sku_id"), e("s.sku_id"), asSet("s"), of("sku_id")),
                pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.art_id"), asSet("a"), of("art_id"), e("s.art_id"), asSet("s"), of("art_id"))
                ), result.getEquiPairs("a"));
        assertEquals(e("a.art_id = s.art_id and sku_id = s.sku_id and active_flg = true and a.internet_flg = false and a.value > 100"), result.getPredicate());
        pair = result.extractPushdownPairs("a");
        assertEquals(asList(
                pair(Type.COMPARISION, ComparisonExpression.Type.GREATER_THAN, e("a.value"), asSet("a"), of("value"), e("100"), asSet(), null),
                pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.internet_flg"), asSet("a"), of("internet_flg"), e("false"), asSet(), null),
                pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("active_flg"), asSet(""), of("active_flg"), e("true"), asSet(), null)
                ),
                pair.getLeft());
        assertEquals(
                result(
                        pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("sku_id"), asSet(""), of("sku_id"), e("s.sku_id"), asSet("s"), of("sku_id")),
                        pair(Type.COMPARISION, ComparisonExpression.Type.EQUAL, e("a.art_id"), asSet("a"), of("art_id"), e("s.art_id"), asSet("s"), of("art_id"))),
                pair.getValue());
        assertEquals(e("a.art_id = s.art_id and sku_id = s.sku_id"), pair.getValue().getPredicate());
        
        result = PredicateAnalyzer.analyze(e("a.art_id = s.art_id or (sku_id = s.sku_id and active_flg and not a.internet_flg and a.value > 100)"));

        assertEquals(
                result(
                        pair(Type.UNDEFINED, null, e("a.art_id = s.art_id or (sku_id = s.sku_id and active_flg and not a.internet_flg and a.value > 100)"), asSet("a", "s", ""), null)),
                result);
    }

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

    private AnalyzeResult result(AnalyzePair... pairs)
    {
        return new AnalyzeResult(asList(pairs));
    }

    private AnalyzePair pair(
            AnalyzePair.Type type,
            ComparisonExpression.Type comparisonType,
            Expression leftExpression,
            Set<String> leftAliases,
            QualifiedName leftQname)
    {
        return new AnalyzePair(
                type,
                comparisonType,
                new AnalyzeItem(leftExpression, leftAliases, leftQname),
                null);
    }

    private AnalyzePair pair(
            AnalyzePair.Type type,
            ComparisonExpression.Type comparisonType,
            Expression leftExpression,
            Set<String> leftAliases,
            QualifiedName leftQname,
            Expression rightExpression,
            Set<String> rightAliases,
            QualifiedName rightQname)
    {
        return new AnalyzePair(
                type,
                comparisonType,
                new AnalyzeItem(leftExpression, leftAliases, leftQname),
                new AnalyzeItem(rightExpression, rightAliases, rightQname));
    }

    private Expression e(String expression)
    {
        return parser.parseExpression(expression);
    }
}
